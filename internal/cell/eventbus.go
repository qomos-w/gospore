package cell

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
)

// subscriptionBufferCap is the per-subscription bounded buffer.
// When full, Publish drops the oldest event and increments the
// subscription's drop counter (surfaceable as a lagging diag).
// 256 keeps each channel preallocation ~18KB (EventEnvelope is 72B), so ~81 live
// subscriptions retain ~1.4MB instead of ~23MB at 4096.
const subscriptionBufferCap = 256

// EventBus is the App-scoped broadcast bus for user-declared events.
// Subscriptions come in three exclusive flavours:
//
//   - SubscribeByService(serviceName, kind) — matches events emitted by
//     any cell that exposes serviceName.
//   - SubscribeByInstance(actorId, kind) — matches events emitted by
//     the specific cell with that canonical actor ID.
//   - SubscribeByKind(kind) — matches events of the given kind emitted
//     by any cell, regardless of identity or service. Use this for
//     infrastructure consumers (e.g. aistats recording every LLM call)
//     that do not care about the source.
//
// Delivery is forward-only (no replay), per-(emitter,kind) FIFO, and
// bounded with drop-oldest backpressure — a slow remote subscriber must
// never stall the producing actor.
//
// Lifecycle:
//   - Publish takes an RLock and iterates a snapshot of current subscribers.
//   - Subscribe takes a Lock and inserts a new subscription into the bucket.
//   - The cancel func closes the subscription's done channel and removes
//     the entry from the bucket; the data channel is never closed by the
//     bus. Consumers select on (ch, done) and exit on done.
//
// EventBus is separate from gospore/events.StoreImpl, which is the
// lifecycle/Watch telemetry bus (operator audience). They do not share
// types intentionally.
type EventBus struct {
	mu            sync.RWMutex
	serviceSubs   map[busKey]map[uint64]*subscription
	instanceSubs  map[busKey]map[uint64]*subscription
	kindSubs      map[string]map[uint64]*subscription
	nextID        uint64
	laggingHook   atomic.Pointer[LaggingHook]
	logger        actor.Logger
}

type busKey struct {
	Value string
	Kind  string
}

// EventEnvelope is the payload delivered to subscribers. SchemaID is
// not included in the envelope: in-process subscribers receive the
// raw Go payload value, and the wire-level subscribe callable (M2)
// resolves SchemaID lazily via the App's schema registry.
type EventEnvelope struct {
	ActorId   string
	Services  []string
	Kind      string
	Payload   any
}

// LaggingHook is invoked once per Publish iteration when EventBus drops
// events because a subscriber's buffer is full. Hooks run on the
// publishing goroutine and must be cheap and non-blocking — they are
// typically used to log or rate-limit observability output keyed by
// actor.DiagEventSubscriberLagging.
type LaggingHook func(LaggingEvent)

// LaggingEvent describes one occurrence of a subscriber falling behind.
// DropsTotal is the cumulative drop count for the subscription at the
// moment the hook fired (snapshot of subscription.drops).
type LaggingEvent struct {
	ActorId    string
	Kind       string
	Identity   id.Identity
	DropsTotal int64
}

// Subscription is a handle returned by Subscribe.
type Subscription struct {
	Ch   <-chan EventEnvelope
	Done <-chan struct{}
	// Drops returns the number of events dropped because this subscription's
	// buffer was full when Publish ran. Reported lazily; suitable for
	// observability, not for flow control.
	Drops func() int64
}

type subscription struct {
	id      uint64
	ident   id.Identity
	ch      chan EventEnvelope
	done    chan struct{}
	drops   atomic.Int64
	created time.Time
}

// NewEventBus constructs an empty EventBus.
func NewEventBus() *EventBus {
	return &EventBus{
		serviceSubs:  make(map[busKey]map[uint64]*subscription),
		instanceSubs: make(map[busKey]map[uint64]*subscription),
		kindSubs:     make(map[string]map[uint64]*subscription),
	}
}

// Publish broadcasts the envelope to every matching subscriber.
// It checks both instance subscriptions (by actorId) and service
// subscriptions (by each serviceName in services).
// Backpressure: if a subscriber's buffer is full, Publish drops the
// oldest queued event for that subscriber and re-attempts; if still
// full, the new event is dropped. Drop counts are tracked per-sub.
//
// Cancelled subscriptions are skipped silently (their done is closed).
func (b *EventBus) Publish(actorId, kind string, services []string, payload any) {
	env := EventEnvelope{ActorId: actorId, Services: services, Kind: kind, Payload: payload}
	b.mu.RLock()

	// Collect all matching subscriptions.
	var subs []*subscription

	// Instance subscribers — match by actorId.
	iKey := busKey{Value: actorId, Kind: kind}
	if bucket, ok := b.instanceSubs[iKey]; ok && len(bucket) > 0 {
		for _, s := range bucket {
			subs = append(subs, s)
		}
	}

	// Service subscribers — match by each exposed service name.
	for _, svc := range services {
		sKey := busKey{Value: svc, Kind: kind}
		if bucket, ok := b.serviceSubs[sKey]; ok && len(bucket) > 0 {
			for _, s := range bucket {
				subs = append(subs, s)
			}
		}
	}

	// Kind subscribers — match any emitter of this kind.
	if bucket, ok := b.kindSubs[kind]; ok && len(bucket) > 0 {
		for _, s := range bucket {
			subs = append(subs, s)
		}
	}

	// Snapshot per-table counts under the same lock: reading the maps
	// after RUnlock would race concurrent unsubscribe deletes.
	instanceCount := len(b.instanceSubs[iKey])
	serviceCount := 0
	for _, svc := range services {
		serviceCount += len(b.serviceSubs[busKey{Value: svc, Kind: kind}])
	}

	b.mu.RUnlock()

	if len(subs) == 0 {
		if b.logger != nil {
			b.logger.Debug("eventbus: Publish dropped — no subscribers", "actorId", actorId, "kind", kind, "services", fmt.Sprintf("%v", services))
		}
		return
	}
	if b.logger != nil {
		b.logger.Debug("eventbus: Publish", "actorId", actorId, "kind", kind, "subscribers", len(subs), "instance", instanceCount, "service", serviceCount)
	}

	for _, s := range subs {
		select {
		case <-s.done:
			continue
		default:
		}

		select {
		case s.ch <- env:
			continue
		case <-s.done:
			continue
		default:
		}

		var dropped bool
		select {
		case <-s.ch:
			s.drops.Add(1)
			dropped = true
		default:
		}
		select {
		case s.ch <- env:
		case <-s.done:
		default:
			s.drops.Add(1)
			dropped = true
		}
		if dropped {
			if hp := b.laggingHook.Load(); hp != nil {
				(*hp)(LaggingEvent{
					ActorId:    actorId,
					Kind:       kind,
					Identity:   s.ident,
					DropsTotal: s.drops.Load(),
				})
			}
		}
	}
}

// SetLogger injects a structured logger into the EventBus.
// When nil, the bus logs silently (no-op).
func (b *EventBus) SetLogger(logger actor.Logger) {
	b.logger = logger
}

// SetLaggingHook installs (or removes, if fn is nil) the hook fired
// when the bus drops events because a subscriber's buffer is full.
func (b *EventBus) SetLaggingHook(fn LaggingHook) {
	if fn == nil {
		b.laggingHook.Store(nil)
		return
	}
	b.laggingHook.Store(&fn)
}

// SubscribeByService registers a listener for events emitted by cells
// that expose serviceName. The subscription is completely independent
// from instance subscriptions — no string collision is possible even if
// a service name happens to look like an actorId.
func (b *EventBus) SubscribeByService(serviceName, kind string, ident id.Identity) (Subscription, func()) {
	return b.subscribe(b.serviceSubs, serviceName, kind, ident)
}

// SubscribeByInstance registers a listener for events emitted by a
// specific cell identified by its canonical actorId.
func (b *EventBus) SubscribeByInstance(actorId, kind string, ident id.Identity) (Subscription, func()) {
	return b.subscribe(b.instanceSubs, actorId, kind, ident)
}

func (b *EventBus) subscribe(table map[busKey]map[uint64]*subscription, value, kind string, ident id.Identity) (Subscription, func()) {
	b.mu.Lock()
	b.nextID++
	subID := b.nextID
	if b.logger != nil {
		b.logger.Info("eventbus: Subscribe", "value", value, "kind", kind, "subID", subID, "role", ident.Role)
	}
	sub := &subscription{
		id:      subID,
		ident:   ident,
		ch:      make(chan EventEnvelope, subscriptionBufferCap),
		done:    make(chan struct{}),
		created: time.Now(),
	}
	key := busKey{Value: value, Kind: kind}
	bucket, ok := table[key]
	if !ok {
		bucket = make(map[uint64]*subscription)
		table[key] = bucket
	}
	bucket[subID] = sub
	b.mu.Unlock()

	var cancelOnce sync.Once
	cancel := func() {
		cancelOnce.Do(func() {
			b.mu.Lock()
			if bucket, ok := table[key]; ok {
				delete(bucket, subID)
				if len(bucket) == 0 {
					delete(table, key)
				}
			}
			b.mu.Unlock()
			close(sub.done)
		})
	}

	handle := Subscription{
		Ch:    sub.ch,
		Done:  sub.done,
		Drops: func() int64 { return sub.drops.Load() },
	}
	return handle, cancel
}

// SubscriberCountByService returns the current number of service-level
// subscribers for (serviceName, kind).
func (b *EventBus) SubscriberCountByService(serviceName, kind string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.serviceSubs[busKey{Value: serviceName, Kind: kind}])
}

// SubscriberCountByInstance returns the current number of instance-level
// subscribers for (actorId, kind).
func (b *EventBus) SubscriberCountByInstance(actorId, kind string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.instanceSubs[busKey{Value: actorId, Kind: kind}])
}

// SubscribeByKind registers a listener for events of the given kind
// emitted by any cell, regardless of identity or service. Use this for
// infrastructure consumers that do not care about the source actor.
func (b *EventBus) SubscribeByKind(kind string, ident id.Identity) (Subscription, func()) {
	b.mu.Lock()
	b.nextID++
	subID := b.nextID
	if b.logger != nil {
		b.logger.Info("eventbus: SubscribeByKind", "kind", kind, "subID", subID, "role", ident.Role)
	}
	sub := &subscription{
		id:      subID,
		ident:   ident,
		ch:      make(chan EventEnvelope, subscriptionBufferCap),
		done:    make(chan struct{}),
		created: time.Now(),
	}
	bucket, ok := b.kindSubs[kind]
	if !ok {
		bucket = make(map[uint64]*subscription)
		b.kindSubs[kind] = bucket
	}
	bucket[subID] = sub
	b.mu.Unlock()

	var cancelOnce sync.Once
	cancel := func() {
		cancelOnce.Do(func() {
			b.mu.Lock()
			if bucket, ok := b.kindSubs[kind]; ok {
				delete(bucket, subID)
				if len(bucket) == 0 {
					delete(b.kindSubs, kind)
				}
			}
			b.mu.Unlock()
			close(sub.done)
		})
	}

	handle := Subscription{
		Ch:    sub.ch,
		Done:  sub.done,
		Drops: func() int64 { return sub.drops.Load() },
	}
	return handle, cancel
}

// SubscriberCountByKind returns the current number of kind-level
// subscribers for kind.
func (b *EventBus) SubscriberCountByKind(kind string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.kindSubs[kind])
}

// Subscribe is kept for backwards compatibility with unit tests.
// It creates an instance subscription.
func (b *EventBus) Subscribe(value, kind string, ident id.Identity) (Subscription, func()) {
	return b.SubscribeByInstance(value, kind, ident)
}

// SubscriberCount is kept for backwards compatibility with unit tests.
func (b *EventBus) SubscriberCount(value, kind string) int {
	return b.SubscriberCountByInstance(value, kind)
}

// SubInfo describes one live subscription for diagnostics: which flavour of
// subscription it is, what it matches, who subscribed, how far behind the
// consumer is, and how many events have been dropped for it.
type SubInfo struct {
	SubID   uint64
	Flavor  string // "service" | "instance" | "kind"
	Match   string // service name or actorId; "" for kind subscriptions
	Kind    string
	Subject string // subscriber identity subject ("" for internal calls)
	Role    string // subscriber identity role
	Backlog int    // events currently buffered, waiting for the consumer
	Drops   int64  // cumulative events dropped because the buffer was full
	Created time.Time
}

// SubscriptionStats returns a point-in-time snapshot of every live
// subscription, deterministically ordered by (flavor, match, kind, subID).
// It exists so the gospore.events.stats callable can surface subscription
// leaks and lagging consumers without reaching into the bus's internals.
func (b *EventBus) SubscriptionStats() []SubInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]SubInfo, 0, len(b.serviceSubs)+len(b.instanceSubs)+len(b.kindSubs))
	appendBucket := func(flavor, match, kind string, bucket map[uint64]*subscription) {
		for _, s := range bucket {
			out = append(out, SubInfo{
				SubID:   s.id,
				Flavor:  flavor,
				Match:   match,
				Kind:    kind,
				Subject: s.ident.Subject,
				Role:    string(s.ident.Role),
				Backlog: len(s.ch),
				Drops:   s.drops.Load(),
				Created: s.created,
			})
		}
	}
	for key, bucket := range b.serviceSubs {
		appendBucket("service", key.Value, key.Kind, bucket)
	}
	for key, bucket := range b.instanceSubs {
		appendBucket("instance", key.Value, key.Kind, bucket)
	}
	for kind, bucket := range b.kindSubs {
		appendBucket("kind", "", kind, bucket)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Flavor != out[j].Flavor {
			return out[i].Flavor < out[j].Flavor
		}
		if out[i].Match != out[j].Match {
			return out[i].Match < out[j].Match
		}
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].SubID < out[j].SubID
	})
	return out
}
