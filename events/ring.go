package events

import (
	"sync"

	"github.com/qomos-w/gospore/actor"
)

// DefaultRingCapacity is the per-actor Event Ring capacity used when
// NewRing receives a non-positive size. Picked to match ARCHITECTURE.md
// §4.23: "每个 actor 默认保留最近 256 条 Record". Cell exposes
// WithEventRingCapacity(n) on App options to override per-app.
const DefaultRingCapacity = 256

// Ring is a per-actor circular buffer of event Records. It is the
// mechanism backing the default events.Store implementation: Cell pushes
// records on every event emission, external subscribers query Recent
// (which calls Since) and Subscribe (which uses LatestSeqNo and
// OldestSeqNo to decide between live-only / replay-then-live / gap
// regimes per ARCHITECTURE.md §4.23).
//
// SeqNo allocation is the caller's responsibility — Cell maintains a
// per-actor monotonic counter and stamps records before Push. Ring
// trusts that incoming SeqNo values are strictly increasing within the
// lifetime of one actor; non-monotonic input is accepted but produces
// undefined Since-query ordering.
//
// Concurrency: writers (Cell goroutine) and readers (Subscribe /
// Recent service callers) are serialised by a single RWMutex — Push
// takes the write lock, Since / OldestSeqNo / LatestSeqNo take the
// read lock. The Ring is small enough that lock-free designs are not
// worth the complexity at this tier.
type Ring struct {
	mu       sync.RWMutex
	buf      []Record
	capacity int
	head     int    // index of oldest record in buf when count > 0
	count    int    // number of records held (0..capacity)
	latest   uint64 // SeqNo of the most recently pushed record (0 if empty)
	oldest   uint64 // SeqNo of the oldest still-held record (0 if empty)
	evicted  uint64 // cumulative records overwritten once the ring filled
}

// NewRing returns an empty Ring with the given capacity. A capacity of
// 0 or negative substitutes DefaultRingCapacity (256) so callers that
// forget to thread the WithEventRingCapacity option still get a sane
// default rather than a degenerate zero-size buffer.
func NewRing(capacity int) *Ring {
	if capacity <= 0 {
		capacity = DefaultRingCapacity
	}
	return &Ring{
		buf:      make([]Record, capacity),
		capacity: capacity,
	}
}

// Capacity returns the maximum number of Records the Ring can hold
// before eviction. Stable for the lifetime of the Ring. nil receiver
// returns 0 so Cell-side gap detection (`since < ring.OldestSeqNo`)
// handles a never-populated actor without a panic.
func (r *Ring) Capacity() int {
	if r == nil {
		return 0
	}
	return r.capacity
}

// Push appends a Record at the tail. If the Ring is full the oldest
// Record is evicted; OldestSeqNo advances to the new oldest. SeqNo is
// taken from the input Record verbatim (Ring does not allocate IDs).
//
// Push is safe to call concurrently with itself only via the Cell
// serialisation guarantee — Cell goroutine is the sole writer. The
// internal lock is taken to make read paths safe against in-progress
// writes, not to make Push itself concurrency-safe.
func (r *Ring) Push(rec Record) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.count < r.capacity {
		idx := (r.head + r.count) % r.capacity
		r.buf[idx] = rec
		r.count++
		if r.count == 1 {
			r.oldest = rec.SeqNo
		}
	} else {
		// Full — overwrite at head, advance head.
		r.buf[r.head] = rec
		r.head = (r.head + 1) % r.capacity
		r.oldest = r.buf[r.head].SeqNo
		r.evicted++
	}
	r.latest = rec.SeqNo
}

// Evicted returns the cumulative number of Records overwritten after
// the Ring first filled. Non-zero means history beyond capacity has
// been silently dropped: Recent/Subscribe callers hitting a gap
// (sinceSeqNo < OldestSeqNo) lost exactly this many records' worth of
// context. Monotonic for the lifetime of the Ring; nil receiver
// returns 0.
func (r *Ring) Evicted() uint64 {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.evicted
}

// Since returns Records with SeqNo strictly greater than sinceSeqNo,
// optionally filtered to the kinds set, in oldest-to-newest order.
//
// Filtering rules:
//   - sinceSeqNo: strict-greater-than. Pass 0 to get every Record
//     currently in the Ring.
//   - kinds: empty / nil slice means "all kinds". Otherwise, a Record
//     matches if its Kind value appears in kinds (set membership, not
//     bitmask AND — each Kind in the slice is a single
//     bit-value already).
//   - limit: > 0 caps the result length; <= 0 returns every match.
//
// The result is a fresh slice owned by the caller — modifying it does
// not affect Ring state. nil receiver returns nil so Recent on a
// never-populated actor returns an empty result without panicking.
//
// Gap detection is the caller's responsibility: when sinceSeqNo <
// OldestSeqNo() the requested range has been evicted; the caller
// (default events.Store) should emit a gospore.events.gap_too_large
// marker and follow with a live-only stream.
func (r *Ring) Since(sinceSeqNo uint64, kinds []actor.WatchKind, limit int) []Record {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.count == 0 {
		return nil
	}

	out := make([]Record, 0, r.count)
	for i := 0; i < r.count; i++ {
		idx := (r.head + i) % r.capacity
		rec := r.buf[idx]
		if rec.SeqNo <= sinceSeqNo {
			continue
		}
		if !kindsMatch(kinds, rec.Kind) {
			continue
		}
		out = append(out, rec)
		if limit > 0 && len(out) == limit {
			break
		}
	}
	return out
}

// OldestSeqNo returns the SeqNo of the oldest Record currently held.
// Returns 0 when the Ring is empty (callers must distinguish "empty"
// from "oldest happens to be 0" via LatestSeqNo or by tracking
// emptiness externally — a fresh actor never publishes SeqNo 0
// because Cell allocates monotonically from 1).
func (r *Ring) OldestSeqNo() uint64 {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.oldest
}

// LatestSeqNo returns the SeqNo of the most recently pushed Record.
// Returns 0 when the Ring has never been pushed to. Used by the
// default Store to decide whether Subscribe(since=N) should replay
// (N < latest), enter live mode straight away (N == latest), or emit
// a gap marker (N < oldest).
func (r *Ring) LatestSeqNo() uint64 {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.latest
}

// Len reports the number of Records currently held. Useful for tests
// and debug/telemetry; the default Store uses Capacity / OldestSeqNo
// / LatestSeqNo for its actual decisions.
func (r *Ring) Len() int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.count
}

// kindsMatch reports whether kind is in the kinds set, or kinds is
// empty (meaning accept all). Kept private — Since is the only caller.
func kindsMatch(kinds []actor.WatchKind, kind actor.WatchKind) bool {
	if len(kinds) == 0 {
		return true
	}
	for _, k := range kinds {
		if k == kind {
			return true
		}
	}
	return false
}
