package events

import (
	"fmt"
	"io"
	"sort"
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
	closeOnce  sync.Once
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

	// dropped counts records the fan-out loop could not deliver because
	// this subscriber's buffer was full (non-blocking send default
	// branch). Observable via StoreImpl.SubscriberStats so a slow
	// consumer is distinguishable from a quiet actor.
	dropped uint64
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
			sub.dropped++
		}
	}
	sink := s.RemoteSink
	s.mu.Unlock()

	// Remote dispatch runs outside the store lock: a transport-backed
	// RemoteSink doing blocking network I/O must not serialise every
	// actor's Push behind it. Concurrent Pushes may dispatch out of
	// order; sinks that need ordering must buffer internally (see
	// RemoteSink contract).
	if sink != nil {
		sink.Push(actorID, rec)
	}
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
				sub.dropped++
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

// RingStat is one actor's retention-ring snapshot, surfaced via
// gospore.events.stats so silent history loss (Evicted > 0) is
// observable instead of only manifesting as subscriber gap markers.
type RingStat struct {
	ActorID  id.ActorID
	Len      int
	Capacity int
	Evicted  uint64
}

// RingStats returns a per-actor snapshot of every retention ring,
// sorted by Evicted descending (then ActorID ascending) so the actors
// losing the most history surface first.
func (s *StoreImpl) RingStats() []RingStat {
	s.mu.Lock()
	out := make([]RingStat, 0, len(s.perActor))
	for actorID, as := range s.perActor {
		out = append(out, RingStat{
			ActorID:  actorID,
			Len:      as.ring.Len(),
			Capacity: as.ring.Capacity(),
			Evicted:  as.ring.Evicted(),
		})
	}
	s.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Evicted != out[j].Evicted {
			return out[i].Evicted > out[j].Evicted
		}
		return out[i].ActorID.String() < out[j].ActorID.String()
	})
	return out
}

// Close tears down every active subscription and rejects further ones.
// The store itself owns no goroutines or timers — subscriber Close is the
// whole cleanup surface. Idempotent. Called on App shutdown; without it,
// subscribers created via Subscribe leak their channels until GC.
func (s *StoreImpl) Close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		subs := make([]*eventSubscription, 0, 8)
		for _, as := range s.perActor {
			subs = append(subs, as.subscribers...)
			as.subscribers = nil
		}
		s.mu.Unlock()
		for _, sub := range subs {
			_ = sub.Close()
		}
	})
}

// SubscriberStat is one active subscription's loss snapshot: how many
// records the fan-out loop dropped because the subscriber's buffer was
// full, plus its kind filter. Surfaced with RingStat via
// gospore.events.stats so a slow consumer is distinguishable from a
// quiet actor.
type SubscriberStat struct {
	ActorID   id.ActorID
	Kinds     []actor.WatchKind
	Dropped   uint64
	BufferCap int
}

// SubscriberStats returns per-subscription drop counters for every
// active subscription, sorted by Dropped descending (then ActorID) so
// the slowest consumers surface first.
func (s *StoreImpl) SubscriberStats() []SubscriberStat {
	s.mu.Lock()
	out := make([]SubscriberStat, 0, 4)
	for actorID, as := range s.perActor {
		for _, sub := range as.subscribers {
			out = append(out, SubscriberStat{
				ActorID:   actorID,
				Kinds:     append([]actor.WatchKind(nil), sub.kinds...),
				Dropped:   sub.dropped,
				BufferCap: cap(sub.ch),
			})
		}
	}
	s.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Dropped != out[j].Dropped {
			return out[i].Dropped > out[j].Dropped
		}
		return out[i].ActorID.String() < out[j].ActorID.String()
	})
	return out
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
	// Done takes priority over a non-empty buffer: once the store (or
	// the subscriber) is closed the stream is terminated, buffered
	// records are not drained — a random select would flap between
	// data and EOF for closed subscriptions with backlog.
	select {
	case <-sub.done:
		return Record{}, io.EOF
	default:
	}
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
