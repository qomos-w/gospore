package tree

import (
	"errors"
	"sync"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/internal/collections"
	"github.com/qomos-w/gospore/ref"
	"github.com/qomos-w/gospore/service"
)

// Allocator is the actor-lifecycle backend Tree drives during Spawn.
//
// Tree itself does not know how to construct actor cells — that lives
// in the Cell tier (M08). The contract is split into phases so Tree's
// write lock is held only for topology reservation, never during actor
// construction or the child's OnInit (which may be arbitrarily slow, or
// may itself spawn grandchildren).
type Allocator interface {
	// Reserve allocates the identity (ActorID + Ref) for a new child
	// WITHOUT constructing the actor. Called while Tree's write lock is
	// held: it must not block, and must not call back into the same Tree.
	Reserve(parent ref.Ref, props actor.Props, name string) (ref.Ref, error)
	// Build constructs the actor for a reserved identity: cell creation,
	// Init delivery, and the OnInit wait. Called WITHOUT the tree lock;
	// may take arbitrarily long and may re-enter Tree (e.g. a child whose
	// OnInit spawns grandchildren). An error means the actor was not
	// built; Tree rolls the reservation back and calls Abort.
	Build(parent ref.Ref, props actor.Props, name string, reserved ref.Ref) error
	// Abort tears down whatever a failed or superseded reservation left
	// behind. Called WITHOUT the tree lock after Build has returned, so
	// it observes the reservation's final state. Must be idempotent.
	Abort(reserved ref.Ref)
}

// AllocatorFuncs adapts three functions to the Allocator interface.
// ReserveFn and BuildFn must be non-nil; AbortFn may be nil.
type AllocatorFuncs struct {
	ReserveFn func(parent ref.Ref, props actor.Props, name string) (ref.Ref, error)
	BuildFn   func(parent ref.Ref, props actor.Props, name string, reserved ref.Ref) error
	AbortFn   func(reserved ref.Ref)
}

func (f *AllocatorFuncs) Reserve(parent ref.Ref, props actor.Props, name string) (ref.Ref, error) {
	return f.ReserveFn(parent, props, name)
}

func (f *AllocatorFuncs) Build(parent ref.Ref, props actor.Props, name string, reserved ref.Ref) error {
	return f.BuildFn(parent, props, name, reserved)
}

func (f *AllocatorFuncs) Abort(reserved ref.Ref) {
	if f.AbortFn != nil {
		f.AbortFn(reserved)
	}
}

// Idler is the cell-idle hook Tree calls inside Stop. It pushes an Idle
// system message into the target cell's mailbox without removing the cell
// from the tree indices. The cell transitions to idle state (OnStop
// called, background work halted) but remains callable and inspectable.
type Idler func(ref.Ref) error

// Terminator is the cell-destroy hook Tree calls inside Destroy.
//
// Tree drives the LIFO descendant-then-target order; for each node,
// the entry is detached from the indices first and Terminator is then
// invoked outside the lock. Terminator performs Cell-tier teardown
// (push Destroy, run OnDestroy, close mailbox, dispose goroutine).
// Errors are accumulated and returned via errors.Join so concurrent
// Lookup / LookupID never observe a half-destroyed tree.
type Terminator func(ref.Ref) error

// Config bundles the inputs Tree needs from the App / Cell tier.
type Config struct {
	Root       ref.Ref
	Allocator  Allocator
	Idler      Idler
	Terminator Terminator
	// LookupGlobalService resolves a global service name to its owning
	// actor ref. Used by RegisterScopedService to detect scoped/global
	// conflicts and by RegisterGlobalService to identify global owners
	// during descendant walks.
	LookupGlobalService func(name string) (ref.Ref, bool)
}

// Sentinel errors returned by Tree operations. They pair with the
// Diag* constants in diag.go for cross-package error-frame
// discrimination.
var (
	// ErrUnknownParent is returned by Spawn when parent is not a
	// live node in this Tree.
	ErrUnknownParent = errors.New("gospore/tree: parent unknown")
	// ErrUnknownTarget is returned by Stop when target is not a
	// live node in this Tree.
	ErrUnknownTarget = errors.New("gospore/tree: target unknown")
	// ErrNameTaken is returned by Spawn when name conflicts with a
	// live sibling under parent. Surfaced cross-package as
	// gospore.tree.name_taken.
	ErrNameTaken = errors.New("gospore/tree: name taken")
	// ErrCycle is returned by Spawn when the Allocator returns a Ref
	// whose ActorID is already known to the Tree (would alias an
	// existing node). Surfaced cross-package as gospore.tree.cycle.
	ErrCycle = errors.New("gospore/tree: would create cycle")
)

// New constructs a Tree from cfg. Returns an error if Root, Allocator,
// Idler, or Terminator is missing.
func New(cfg Config) (Tree, error) {
	if cfg.Root == nil {
		return nil, errors.New("gospore/tree: Config.Root required")
	}
	if cfg.Allocator == nil {
		return nil, errors.New("gospore/tree: Config.Allocator required")
	}
	if cfg.Idler == nil {
		return nil, errors.New("gospore/tree: Config.Idler required")
	}
	if cfg.Terminator == nil {
		return nil, errors.New("gospore/tree: Config.Terminator required")
	}
	rootNode := &node{
		self:     cfg.Root,
		children: map[string]*node{},
	}
	t := &tree{
		cfg:             cfg,
		rootNode:        rootNode,
		byID:            map[id.ActorID]*node{cfg.Root.ID(): rootNode},
		scopedServices:  make(map[ref.Ref]map[string]ref.Ref),
		globalServices:  make(map[ref.Ref]*collections.Set[string]),
	}
	return t, nil
}

type node struct {
	self     ref.Ref
	parent   *node
	children map[string]*node
	name     string
	// pending marks a node whose actor is still being built by an
	// in-flight Spawn (Phase 2). Pending nodes hold the name slot and the
	// ActorID, but are invisible to Walk/Children/Parent/LookupID/Stop
	// until the spawner commits. Destroy sweeps them like any other node.
	pending bool
}

type tree struct {
	mu       sync.RWMutex
	cfg      Config
	rootNode *node
	byID     map[id.ActorID]*node
	onChange []func()

	// scopedServices maps owner ref to registered scoped service names.
	scopedServices map[ref.Ref]map[string]ref.Ref
	// globalServices tracks which actors have registered global services
	// so that scoped/global conflicts can be detected under the tree lock.
	globalServices map[ref.Ref]*collections.Set[string]
}

func (t *tree) Root() ref.Ref { return t.cfg.Root }

func (t *tree) OnChange(callback func()) {
	t.mu.Lock()
	t.onChange = append(t.onChange, callback)
	t.mu.Unlock()
}

// fireChange invokes registered callbacks outside the write lock.
func (t *tree) fireChange() {
	t.mu.RLock()
	cbs := make([]func(), len(t.onChange))
	copy(cbs, t.onChange)
	t.mu.RUnlock()
	for _, cb := range cbs {
		cb()
	}
}

func (t *tree) Spawn(parent ref.Ref, props actor.Props, name string) (ref.Ref, error) {
	if parent == nil {
		return nil, ErrUnknownParent
	}

	// Phase 1 — reserve (write lock held). Only identity allocation, the
	// name-conflict check and topology insertion happen under the lock.
	// The child is inserted as a pending node: invisible to readers, but
	// holding the name slot and the ActorID, so a concurrent Spawn with
	// the same name or the same explicit ID fails fast.
	t.mu.Lock()
	pNode, ok := t.byID[parent.ID()]
	if !ok {
		t.mu.Unlock()
		return nil, ErrUnknownParent
	}
	if _, dup := pNode.children[name]; dup {
		t.mu.Unlock()
		return nil, ErrNameTaken
	}
	childRef, err := t.cfg.Allocator.Reserve(parent, props, name)
	if err != nil {
		t.mu.Unlock()
		return nil, err
	}
	if childRef == nil {
		t.mu.Unlock()
		return nil, errors.New("gospore/tree: allocator returned nil ref")
	}
	if _, exists := t.byID[childRef.ID()]; exists {
		t.mu.Unlock()
		return nil, ErrCycle
	}
	childNode := &node{
		self:     childRef,
		parent:   pNode,
		children: map[string]*node{},
		name:     name,
		pending:  true,
	}
	pNode.children[name] = childNode
	t.byID[childRef.ID()] = childNode
	t.mu.Unlock()

	// Phase 2 — build (no lock). Cell construction, Init delivery and the
	// OnInit wait run outside the write lock, so a slow OnInit cannot
	// stall unrelated spawns, and an OnInit that spawns grandchildren
	// cannot deadlock against this Spawn.
	buildErr := t.cfg.Allocator.Build(parent, props, name, childRef)

	// Phase 3 — commit or roll back (write lock held).
	t.mu.Lock()
	ours, oursOK := t.byID[childRef.ID()]
	stillParent := t.byID[parent.ID()] == pNode
	if buildErr == nil && stillParent && oursOK && ours == childNode {
		childNode.pending = false
		t.mu.Unlock()
		t.fireChange()
		return childRef, nil
	}
	var order []*node
	if oursOK && ours == childNode {
		// Nobody tore the reservation down externally: detach the whole
		// pending subtree (grandchildren spawned during Build may have
		// committed under it).
		order = collectPostOrder(childNode)
		for _, x := range order {
			delete(t.byID, x.self.ID())
			delete(t.scopedServices, x.self)
			delete(t.globalServices, x.self)
		}
		if childNode.parent != nil {
			delete(childNode.parent.children, childNode.name)
		}
	}
	t.mu.Unlock()
	// Terminators run outside the lock, best-effort: the primary error is
	// the spawn failure, not the teardown.
	for _, x := range order {
		_ = t.cfg.Terminator(x.self)
	}
	t.cfg.Allocator.Abort(childRef)
	if buildErr != nil {
		return nil, buildErr
	}
	if !stillParent {
		return nil, ErrUnknownParent
	}
	return nil, ErrUnknownTarget
}

func (t *tree) Stop(target ref.Ref) error {
	if target == nil {
		return ErrUnknownTarget
	}
	t.mu.RLock()
	n, ok := t.byID[target.ID()]
	pending := ok && n.pending
	t.mu.RUnlock()
	if !ok || pending {
		return ErrUnknownTarget
	}
	return t.cfg.Idler(target)
}

func (t *tree) Destroy(target ref.Ref) error {
	if target == nil {
		return ErrUnknownTarget
	}
	t.mu.Lock()
	n, ok := t.byID[target.ID()]
	if !ok {
		t.mu.Unlock()
		return ErrUnknownTarget
	}
	order := collectPostOrder(n)
	for _, x := range order {
		delete(t.byID, x.self.ID())
		delete(t.scopedServices, x.self)
		delete(t.globalServices, x.self)
	}
	if n.parent != nil {
		delete(n.parent.children, n.name)
	}
	t.mu.Unlock()
	var errs []error
	for _, x := range order {
		if err := t.cfg.Terminator(x.self); err != nil {
			errs = append(errs, err)
		}
	}
	t.fireChange()
	return errors.Join(errs...)
}

func collectPostOrder(root *node) []*node {
	var order []*node
	var visit func(*node)
	visit = func(x *node) {
		names := sortedChildNames(x)
		for _, cn := range names {
			visit(x.children[cn])
		}
		order = append(order, x)
	}
	visit(root)
	return order
}

func (t *tree) Children(parent ref.Ref) []ref.Ref {
	if parent == nil {
		return nil
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	n, ok := t.byID[parent.ID()]
	if !ok {
		return nil
	}
	names := sortedChildNames(n)
	out := make([]ref.Ref, 0, len(names))
	for _, cn := range names {
		if n.children[cn].pending {
			continue
		}
		out = append(out, n.children[cn].self)
	}
	return out
}

func (t *tree) Parent(child ref.Ref) (ref.Ref, bool) {
	if child == nil {
		return nil, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	n, ok := t.byID[child.ID()]
	if !ok || n.pending || n.parent == nil {
		return nil, false
	}
	return n.parent.self, true
}


func (t *tree) LookupID(aid id.ActorID) (ref.Ref, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	n, ok := t.byID[aid]
	if !ok || n.pending {
		return nil, false
	}
	return n.self, true
}

func (t *tree) Walk(visit func(ref.Ref) bool) {
	if visit == nil {
		return
	}
	var snapshot []ref.Ref
	t.mu.RLock()
	var walk func(*node)
	walk = func(x *node) {
		if x.pending {
			// The whole pending subtree stays hidden until its root
			// commits — no partially-visible intermediate states.
			return
		}
		snapshot = append(snapshot, x.self)
		for _, cn := range sortedChildNames(x) {
			walk(x.children[cn])
		}
	}
	walk(t.rootNode)
	t.mu.RUnlock()
	for _, r := range snapshot {
		if !visit(r) {
			return
		}
	}
}

func (t *tree) RegisterScopedService(owner ref.Ref, name string, target ref.Ref) error {
	if owner == nil {
		return ErrUnknownTarget
	}
	if !service.ValidName(name) {
		return service.ErrInvalidServiceName
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.byID[owner.ID()]; !ok {
		return ErrUnknownTarget
	}
	if t.anyGlobalService(name) {
		return service.ErrScopedConflict
	}
	m, ok := t.scopedServices[owner]
	if !ok {
		m = make(map[string]ref.Ref)
		t.scopedServices[owner] = m
	}
	if _, exists := m[name]; exists {
		return service.ErrScopedConflict
	}
	m[name] = target
	return nil
}

func (t *tree) UnregisterScopedService(owner ref.Ref, name string) {
	if owner == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if m, ok := t.scopedServices[owner]; ok {
		delete(m, name)
		if len(m) == 0 {
			delete(t.scopedServices, owner)
		}
	}
}

func (t *tree) UnregisterAllScopedServices(owner ref.Ref) {
	if owner == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.scopedServices, owner)
}

func (t *tree) LookupScopedService(caller ref.Ref, name string) (ref.Ref, bool) {
	if caller == nil {
		return nil, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	n, ok := t.byID[caller.ID()]
	if !ok {
		return nil, false
	}
	for p := n.parent; p != nil; p = p.parent {
		if m, ok := t.scopedServices[p.self]; ok {
			if r, ok2 := m[name]; ok2 {
				return r, true
			}
		}
	}
	return nil, false
}

func (t *tree) RegisterGlobalService(owner ref.Ref, name string, target ref.Ref) error {
	if owner == nil {
		return ErrUnknownTarget
	}
	if !service.ValidName(name) {
		return service.ErrInvalidServiceName
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.byID[owner.ID()]; !ok {
		return ErrUnknownTarget
	}
	for o, names := range t.globalServices {
		if names.Has(name) {
			if o == owner {
				return nil
			}
			return service.ErrServiceNameTaken
		}
	}
	if t.anyScopedService(name) {
		return service.ErrScopedConflict
	}
	m, ok := t.globalServices[owner]
	if !ok {
		m = collections.NewSet[string](1)
		t.globalServices[owner] = m
	}
	m.Add(name)
	return nil
}

func (t *tree) UnregisterGlobalService(owner ref.Ref, name string) {
	if owner == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if m, ok := t.globalServices[owner]; ok {
		m.Delete(name)
		if m.Len() == 0 {
			delete(t.globalServices, owner)
		}
	}
}

func (t *tree) anyGlobalService(name string) bool {
	for _, names := range t.globalServices {
		if names.Has(name) {
			return true
		}
	}
	return false
}

func (t *tree) anyScopedService(name string) bool {
	for _, m := range t.scopedServices {
		if _, ok := m[name]; ok {
			return true
		}
	}
	return false
}

func sortedChildNames(n *node) []string {
	return collections.SortedKeys(n.children)
}
