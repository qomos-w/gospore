package discovery

import (
	"cmp"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/qomos-w/gospore/internal/collections"
)

// ErrNotFound is returned by Renew or Deregister when the target
// namespace is not in the live set.
var ErrNotFound = errors.New("discovery: instance not found")

// MemoryProvider is the default in-memory discovery implementation.
// Suitable for single-process Phase 1 deployments.
//
// Time is explicit: Advance must be called (or the App's clock must
// tick) to move lease expiry forward. This makes tests deterministic
// and avoids wall-clock dependency.
type MemoryProvider struct {
	mu        sync.Mutex
	now       time.Time
	nextSeq   uint64
	instances map[string]Instance
	expired   *collections.Set[string]
	watch     *watchHub
}

// NewMemoryProvider creates an in-memory provider starting at the
// given time. The zero value is not usable — always construct via
// this constructor.
func NewMemoryProvider(now time.Time) *MemoryProvider {
	return &MemoryProvider{
		now:       now,
		nextSeq:   1,
		instances: map[string]Instance{},
		expired:   collections.NewSet[string](0),
		watch:     newWatchHub(256),
	}
}

// Register adds instance to the live set with the given TTL.
// Re-registration of a previously-expired namespace emits
// EventRejoined rather than EventRegistered.
func (p *MemoryProvider) Register(instance Instance, ttl time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, existed := p.instances[instance.Namespace]
	wasExpired := p.expired.Has(instance.Namespace)
	instance.LeaseUntil = p.now.Add(ttl)
	p.instances[instance.Namespace] = instance
	p.expired.Delete(instance.Namespace)
	kind := EventRegistered
	if !existed && wasExpired {
		kind = EventRejoined
	}
	p.publishLocked(kind, instance)
	return nil
}

// Renew extends the lease for the instance bound to namespace.
func (p *MemoryProvider) Renew(namespace string, ttl time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	instance, ok := p.instances[namespace]
	if !ok {
		return ErrNotFound
	}
	instance.LeaseUntil = p.now.Add(ttl)
	p.instances[namespace] = instance
	p.publishLocked(EventRenewed, instance)
	return nil
}

// Deregister explicitly removes the instance bound to namespace.
func (p *MemoryProvider) Deregister(namespace string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	instance, ok := p.instances[namespace]
	if !ok {
		return ErrNotFound
	}
	delete(p.instances, namespace)
	p.expired.Add(namespace)
	p.publishLocked(EventExpired, instance)
	return nil
}

// Resolve returns the current live instance set for service, sorted
// by namespace for deterministic output.
func (p *MemoryProvider) Resolve(service string) []Instance {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Instance, 0)
	for _, instance := range p.instances {
		if instance.Service == service {
			out = append(out, instance)
		}
	}
	slices.SortFunc(out, func(a, b Instance) int { return cmp.Compare(a.Namespace, b.Namespace) })
	return out
}

// Subscribe returns a Watcher whose channel receives lifecycle Events.
// If since > 0 the Watcher first replays events with SeqNo > since
// from the event ring, then transitions to live delivery.
func (p *MemoryProvider) Subscribe(since uint64) (Watcher, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.watch.subscribe(since), nil
}

// Advance moves the internal clock to now and expires any leases
// whose LeaseUntil has elapsed. Returns the expired instances in
// deterministic (namespace-sorted) order.
func (p *MemoryProvider) Advance(now time.Time) []Instance {
	p.mu.Lock()
	defer p.mu.Unlock()
	if now.Before(p.now) {
		now = p.now
	}
	p.now = now
	var expired []Instance
	for namespace, instance := range p.instances {
		if instance.LeaseUntil.After(p.now) {
			continue
		}
		delete(p.instances, namespace)
		p.expired.Add(namespace)
		expired = append(expired, instance)
		p.publishLocked(EventExpired, instance)
	}
	slices.SortFunc(expired, func(a, b Instance) int { return cmp.Compare(a.Namespace, b.Namespace) })
	return expired
}

func (p *MemoryProvider) publishLocked(kind EventKind, instance Instance) {
	event := Event{
		SeqNo:    p.nextSeq,
		Kind:     kind,
		Service:  instance.Service,
		Instance: instance,
	}
	p.nextSeq++
	p.watch.publish(event)
}

// watchHub is the internal fan-out mechanism for Subscribe.
type watchHub struct {
	mu          sync.Mutex
	bufferLimit int
	events      []Event
	watchers    []chan Event
	dropped     uint64
}

func newWatchHub(bufferLimit int) *watchHub {
	return &watchHub{bufferLimit: bufferLimit, events: []Event{}, watchers: []chan Event{}}
}

func (h *watchHub) subscribe(since uint64) Watcher {
	h.mu.Lock()
	defer h.mu.Unlock()
	ch := make(chan Event, 32)
	for _, event := range h.events {
		if event.SeqNo > since {
			ch <- event
		}
	}
	h.watchers = append(h.watchers, ch)
	return Watcher{
		C: ch,
		stop: func() { h.unsubscribe(ch) },
	}
}

func (h *watchHub) unsubscribe(ch chan Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, w := range h.watchers {
		if w == ch {
			h.watchers = append(h.watchers[:i], h.watchers[i+1:]...)
			close(ch)
			return
		}
	}
}

func (h *watchHub) publish(event Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, event)
	if len(h.events) > h.bufferLimit {
		h.events = append([]Event(nil), h.events[len(h.events)-h.bufferLimit:]...)
	}
	alive := h.watchers[:0]
	for _, watcher := range h.watchers {
		select {
		case watcher <- event:
			alive = append(alive, watcher)
		default:
			h.dropped++
		}
	}
	h.watchers = alive
}
