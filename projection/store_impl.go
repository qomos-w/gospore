package projection

import (
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/spore/binding"
)

type StoreImpl struct {
	mu       sync.Mutex
	perActor map[id.ActorID]*storeActorState
	deltaCap int
}

type storeActorState struct {
	snapshot    Snapshot
	window      *DeltaWindow
	subscribers []*projectionSubscription
}

type projectionSubscription struct {
	mu           sync.Mutex
	ch           chan Update
	done         chan struct{}
	closeOnce    sync.Once
	cleanup      func()
	batchTimeout time.Duration
	pending      *Update
	timer        *time.Timer
}

func NewStore(deltaCap int) *StoreImpl {
	if deltaCap <= 0 {
		deltaCap = DefaultDeltaWindowCapacity
	}
	return &StoreImpl{
		perActor: make(map[id.ActorID]*storeActorState),
		deltaCap: deltaCap,
	}
}

// Delete removes an actor's projection state and terminates its
// subscribers. Ephemeral actors (e.g. plan nodes wrapping a single
// invoke) call it on teardown so their final snapshots do not
// accumulate in the store for the process lifetime.
func (s *StoreImpl) Delete(actorID id.ActorID) {
	s.mu.Lock()
	as, ok := s.perActor[actorID]
	if !ok {
		s.mu.Unlock()
		return
	}
	delete(s.perActor, actorID)
	subs := as.subscribers
	as.subscribers = nil
	s.mu.Unlock()

	// Close outside the store lock: subscription cleanup re-acquires it.
	for _, sub := range subs {
		_ = sub.Close()
	}
}

func (s *StoreImpl) getOrCreateLocked(actorID id.ActorID) *storeActorState {
	as, ok := s.perActor[actorID]
	if !ok {
		as = &storeActorState{window: NewDeltaWindow(s.deltaCap)}
		s.perActor[actorID] = as
	}
	return as
}

func (s *StoreImpl) Get(actorID id.ActorID) (Snapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	as, ok := s.perActor[actorID]
	if !ok || len(as.snapshot.Fields) == 0 {
		return Snapshot{}, false
	}
	return cloneSnapshot(as.snapshot), true
}

func (s *StoreImpl) GetField(actorID id.ActorID, name string) (binding.ViewProjection, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	as, ok := s.perActor[actorID]
	if !ok || len(as.snapshot.Fields) == 0 {
		return binding.ViewProjection{}, false
	}
	field, ok := as.snapshot.Fields[name]
	if !ok {
		return binding.ViewProjection{}, false
	}
	return cloneViewProjection(field), true
}

func (s *StoreImpl) Watch(actorID id.ActorID, opts ...WatchOption) (actor.Subscription[Update], error) {
	cfg := watchConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	sub := &projectionSubscription{
		ch:           make(chan Update, 16),
		done:         make(chan struct{}),
		batchTimeout: cfg.batchTimeout,
	}
	var initial []Update

	s.mu.Lock()
	as := s.getOrCreateLocked(actorID)
	if len(as.snapshot.Fields) != 0 {
		if cfg.hasSinceSet && cfg.sinceVersion > 0 {
			oldest := as.window.OldestVersion()
			latest := as.window.LatestVersion()
			switch {
			case oldest > 0 && cfg.sinceVersion < oldest:
				initial = append(initial, Update{Kind: UpdateGapTooLarge, Version: as.snapshot.Version, Snapshot: cloneSnapshot(as.snapshot)})
				initial = append(initial, Update{Kind: UpdateFullSnapshot, Version: as.snapshot.Version, Snapshot: cloneSnapshot(as.snapshot)})
			case cfg.sinceVersion < latest:
				initial = append(initial, cloneUpdates(as.window.Since(cfg.sinceVersion, 0))...)
			case cfg.sinceVersion >= latest:
			}
		} else {
			initial = append(initial, Update{Kind: UpdateFullSnapshot, Version: as.snapshot.Version, Snapshot: cloneSnapshot(as.snapshot)})
		}
	}
	for _, up := range initial {
		sub.ch <- up
	}
	sub.cleanup = func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for i, ss := range as.subscribers {
			if ss == sub {
				as.subscribers = append(as.subscribers[:i], as.subscribers[i+1:]...)
				break
			}
		}
	}
	as.subscribers = append(as.subscribers, sub)
	s.mu.Unlock()
	return sub, nil
}

func (s *StoreImpl) Has(actorID id.ActorID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	as, ok := s.perActor[actorID]
	return ok && len(as.snapshot.Fields) > 0
}

func (s *StoreImpl) HasSubscribers(actorID id.ActorID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	as, ok := s.perActor[actorID]
	if !ok {
		return false
	}
	return len(as.subscribers) > 0
}

func (s *StoreImpl) Publish(actorID id.ActorID, snap Snapshot) {
	s.mu.Lock()
	as := s.getOrCreateLocked(actorID)
	prev := as.snapshot
	as.snapshot = cloneSnapshot(snap)
	if snap.Version > 0 {
		delta := computeDeltaUpdate(prev, snap)
		as.window.Push(delta)
		for _, sub := range as.subscribers {
			sub.deliver(delta)
		}
	}
	s.mu.Unlock()
}

func computeDeltaUpdate(prev, next Snapshot) Update {
	delta := make(map[string]binding.ViewProjection)
	for name, field := range next.Fields {
		prevField, ok := prev.Fields[name]
		if !ok || !equalViewProjection(prevField, field) {
			delta[name] = cloneViewProjection(field)
		}
	}
	return Update{Kind: UpdateDelta, Version: next.Version, PrevVersion: prev.Version, Delta: delta}
}

func equalViewProjection(a, b binding.ViewProjection) bool {
	if len(a.Fields) != len(b.Fields) {
		return false
	}
	for k, av := range a.Fields {
		bv, ok := b.Fields[k]
		if !ok || !reflect.DeepEqual(av, bv) {
			return false
		}
	}
	return true
}

func cloneUpdates(in []Update) []Update {
	if len(in) == 0 {
		return nil
	}
	out := make([]Update, len(in))
	for i, up := range in {
		out[i] = Update{Kind: up.Kind, Version: up.Version, PrevVersion: up.PrevVersion, Snapshot: cloneSnapshot(up.Snapshot)}
		if len(up.Delta) != 0 {
			out[i].Delta = make(map[string]binding.ViewProjection, len(up.Delta))
			for k, v := range up.Delta {
				out[i].Delta[k] = cloneViewProjection(v)
			}
		}
	}
	return out
}

func (sub *projectionSubscription) deliver(up Update) {
	if sub.batchTimeout <= 0 || up.Kind != UpdateDelta {
		sub.sendNow(up)
		return
	}

	sub.mu.Lock()
	defer sub.mu.Unlock()
	if sub.isDoneLocked() {
		return
	}
	if sub.pending == nil {
		copy := cloneUpdate(up)
		sub.pending = &copy
		sub.timer = time.AfterFunc(sub.batchTimeout, sub.flushPending)
		return
	}
	mergePending(sub.pending, up)
}

func (sub *projectionSubscription) flushPending() {
	sub.mu.Lock()
	if sub.pending == nil || sub.isDoneLocked() {
		sub.mu.Unlock()
		return
	}
	up := cloneUpdate(*sub.pending)
	sub.pending = nil
	sub.timer = nil
	ch := sub.ch
	done := sub.done
	sub.mu.Unlock()

	select {
	case ch <- up:
	case <-done:
	default:
	}
}

func (sub *projectionSubscription) sendNow(up Update) {
	sub.mu.Lock()
	if sub.isDoneLocked() {
		sub.mu.Unlock()
		return
	}
	ch := sub.ch
	done := sub.done
	sub.mu.Unlock()

	select {
	case ch <- cloneUpdate(up):
	case <-done:
	default:
	}
}

func (sub *projectionSubscription) isDoneLocked() bool {
	select {
	case <-sub.done:
		return true
	default:
		return false
	}
}

func mergePending(dst *Update, next Update) {
	if dst == nil {
		return
	}
	if dst.Kind != UpdateDelta || next.Kind != UpdateDelta {
		*dst = cloneUpdate(next)
		return
	}
	if dst.Delta == nil {
		dst.Delta = map[string]binding.ViewProjection{}
	}
	for k, v := range next.Delta {
		dst.Delta[k] = cloneViewProjection(v)
	}
	dst.Version = next.Version
}

func cloneUpdate(up Update) Update {
	out := Update{Kind: up.Kind, Version: up.Version, PrevVersion: up.PrevVersion, Snapshot: cloneSnapshot(up.Snapshot)}
	if len(up.Delta) != 0 {
		out.Delta = make(map[string]binding.ViewProjection, len(up.Delta))
		for k, v := range up.Delta {
			out.Delta[k] = cloneViewProjection(v)
		}
	}
	return out
}

func (sub *projectionSubscription) Recv() (Update, error) {
	select {
	case up, ok := <-sub.ch:
		if !ok {
			return Update{}, io.EOF
		}
		return up, nil
	case <-sub.done:
		return Update{}, io.EOF
	}
}

func (sub *projectionSubscription) Close() error {
	sub.closeOnce.Do(func() {
		sub.mu.Lock()
		if sub.timer != nil {
			sub.timer.Stop()
			sub.timer = nil
		}
		sub.pending = nil
		sub.mu.Unlock()
		if sub.cleanup != nil {
			sub.cleanup()
		}
		close(sub.done)
	})
	return nil
}

func (sub *projectionSubscription) Done() <-chan struct{} {
	return sub.done
}

func cloneSnapshot(in Snapshot) Snapshot {
	out := Snapshot{ActorID: in.ActorID, Version: in.Version}
	if len(in.Fields) != 0 {
		out.Fields = make(map[string]binding.ViewProjection, len(in.Fields))
		for k, v := range in.Fields {
			out.Fields[k] = cloneViewProjection(v)
		}
	}
	return out
}

func cloneViewProjection(in binding.ViewProjection) binding.ViewProjection {
	out := in
	if len(in.Fields) != 0 {
		out.Fields = make(map[string]any, len(in.Fields))
		for k, v := range in.Fields {
			out.Fields[k] = v
		}
	}
	return out
}

var _ Store = (*StoreImpl)(nil)
var _ actor.Subscription[Update] = (*projectionSubscription)(nil)
