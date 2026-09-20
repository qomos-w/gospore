// Package resource is the App-scoped, type-safe registry for
// startup-shared infrastructure (db, redis, http client, config, etc.).
//
// Lifecycle:
//  1. Set during RootActor.OnStart (or via app.WithResource).
//  2. Frozen by gospore between the last OnStart and the first mailbox
//     Pop; any Set after freeze returns ErrFrozen.
//  3. Read-only via ctx.Resources() throughout the running App.
//  4. gospore does NOT close resources at shutdown; the user closes
//     them in RootActor.OnStop.
//
// Resource is NOT a DI container — there is no resolution graph, no
// scopes, no cycle detection. Complex wiring still belongs in
// RootActor closures or service actors. Resource exists to spare the
// few globally-shared values the cost of prop-drilling.
package resource

import (
	"errors"
	"sync"
	"sync/atomic"
)

// Registry is the type-safe resource table.
//
// The interface uses any keys; package-level generic Set / Get /
// MustGet wrappers provide the type-safe surface most callers want.
type Registry interface {
	// Set binds value to key. Returns ErrFrozen after freeze.
	Set(key any, value any) error
	// Get retrieves the value bound to key.
	Get(key any) (any, bool)
	// Has reports whether key is bound.
	Has(key any) bool
	// Frozen reports whether further Set is rejected.
	Frozen() bool
}

// ErrFrozen is returned by Set after the registry has been frozen.
var ErrFrozen = errors.New("gospore/resource: registry frozen")

// ErrTypeMismatch surfaces from generic Get / MustGet when the
// stored value's runtime type does not match T.
var ErrTypeMismatch = errors.New("gospore/resource: type mismatch")

// New constructs a fresh, mutable Registry. Callers must invoke
// Freeze before the App enters its mailbox Pop loop; subsequent
// Set calls then return ErrFrozen.
func New() Registry {
	return &registry{m: make(map[any]any)}
}

// Freeze flips r into read-only mode. Subsequent Set calls return
// ErrFrozen. Called by gospore-internal app boot after RootActor.OnStart
// chains complete; user code should not call Freeze.
//
// Idempotent: multiple invocations are no-ops. Safe to call from any
// goroutine.
func Freeze(r Registry) {
	if reg, ok := r.(*registry); ok {
		reg.freeze()
	}
}

// registry is the concrete Registry. Pre-freeze, Set/Get/Has share an
// RWMutex. The frozen flag is an atomic.Bool so Frozen() and the
// fast-path of Set can avoid the mutex.
type registry struct {
	mu     sync.RWMutex
	m      map[any]any
	frozen atomic.Bool
}

func (r *registry) Set(key any, value any) error {
	if r.frozen.Load() {
		return ErrFrozen
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// Re-check under lock so a concurrent freeze() that landed between
	// the atomic load and Lock acquisition still rejects this write.
	if r.frozen.Load() {
		return ErrFrozen
	}
	r.m[key] = value
	return nil
}

func (r *registry) Get(key any) (any, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.m[key]
	return v, ok
}

func (r *registry) Has(key any) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.m[key]
	return ok
}

func (r *registry) Frozen() bool {
	return r.frozen.Load()
}

func (r *registry) freeze() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.frozen.Store(true)
}
