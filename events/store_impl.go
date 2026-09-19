package events

import (
	"fmt"
	"io"
	"sync"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
)

// StoreImpl is the default in-memory events store. Cell pushes Records via
// Push; external observers query via Recent and subscribe via Subscribe.
type StoreImpl struct {
	mu         sync.Mutex
	perActor   map[id.ActorID]*actorState
	ringCap    int
	RemoteSink RemoteSink // optional; when nil, only in-process delivery
}

type actorState struct {
	ring        *Ring
	subscribers []*eventSubscription
	nextSeqNo   uint64
}

type eventSubscription struct {
	ch        chan Record
	done      chan struct{}
	kinds     []actor.WatchKind
	closeOnce sync.Once
	cleanup   func()
}

// NewStore creates a new in-memory events store. ringCap sets the per-actor
// ring capacity; a non-positive value substitutes DefaultRingCapacity (256).
func NewStore(ringCap int) *StoreImpl {
	if ringCap <= 0 {
		ringCap = DefaultRingCapacity
	}
	return &StoreImpl{
		perActor: make(map[id.ActorID]*actorState),
		ringCap:  ringCap,
	}
}

func (s *StoreImpl) getOrCreateLocked(actorID id.ActorID) *actorState {
	as, ok := s.perActor[actorID]
	if !ok {
		as = &actorState{
			ring: NewRing(s.ringCap),
		}
		s.perActor[actorID] = as
	}
	return as
}

// Push adds a Record to the actor's ring and fans out to active subscribers.
// SeqNo is assigned internally from a per-actor monotonic counter (starting at
// 1); the caller's SeqNo is overwritten.
func (s *StoreImpl) Push(actorID id.ActorID, rec Record) {
	s.mu.Lock()
	as := s.getOrCreateLocked(actorID)
	as.nextSeqNo++
	rec.SeqNo = as.nextSeqNo
	as.ring.Push(rec)

	for _, sub := range as.subscribers {
		if len(sub.kinds) > 0 && !kindsMatch(sub.kinds, rec.Kind) {
			continue
		}
		select {
		case sub.ch <- rec:
		default:
		}
	}
	if s.RemoteSink != nil {
		s.RemoteSink.Push(actorID, rec)
	}
	s.mu.Unlock()
}

// Recent returns historical Records from the actor's ring.
func (s *StoreImpl) Recent(actorID id.ActorID, sinceSeqNo uint64, kinds []actor.WatchKind, limit int) []Record {
	s.mu.Lock()
	as, ok := s.perActor[actorID]
	s.mu.Unlock()
	if !ok {
		return nil
	}
	return as.ring.Since(sinceSeqNo, kinds, limit)
}

// Subscribe creates a live subscription. Replays existing records from the
// ring (if sinceSeqNo > 0), then transitions to live delivery. Returns an
// error if any kind in the kinds slice is not a valid WatchKind constant.
func (s *StoreImpl) Subscribe(actorID id.ActorID, sinceSeqNo uint64, kinds []actor.WatchKind) (actor.Subscription[Record], error) {
	for _, k := range kinds {
		if !actor.ValidWatchKind(k) {
			return nil, fmt.Errorf("%s: %d is not a valid WatchKind", DiagInvalidKinds, k)
		}
	}

	sub := &eventSubscription{
		ch:    make(chan Record, 16),
		done:  make(chan struct{}),
		kinds: kinds,
	}

	s.mu.Lock()
	as := s.getOrCreateLocked(actorID)

	if sinceSeqNo > 0 {
		for _, rec := range as.ring.Since(sinceSeqNo, kinds, 0) {
			select {
			case sub.ch <- rec:
			default:
			}
		}
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

// HasSubscribers reports whether the actor has any active subscriptions.
func (s *StoreImpl) HasSubscribers(actorID id.ActorID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	as, ok := s.perActor[actorID]
	if !ok {
		return false
	}
	return len(as.subscribers) > 0
}

func (sub *eventSubscription) Recv() (Record, error) {
	select {
	case rec, ok := <-sub.ch:
		if !ok {
			return Record{}, io.EOF
		}
		return rec, nil
	case <-sub.done:
		return Record{}, io.EOF
	}
}

func (sub *eventSubscription) Close() error {
	sub.closeOnce.Do(func() {
		if sub.cleanup != nil {
			sub.cleanup()
		}
		close(sub.done)
	})
	return nil
}

func (sub *eventSubscription) Done() <-chan struct{} {
	return sub.done
}

var _ Store = (*StoreImpl)(nil)
var _ actor.Subscription[Record] = (*eventSubscription)(nil)
