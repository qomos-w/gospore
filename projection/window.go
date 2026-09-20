package projection

import "sync"

// DefaultDeltaWindowCapacity is the per-actor Delta Window capacity used
// when NewDeltaWindow receives a non-positive size. Picked to match
// ARCHITECTURE.md §4.18: "WithProjectionDeltaWindow(n int) Option //
// 默认 64：保留最近 N 个 delta 用于重连补全". Cell exposes
// WithProjectionDeltaWindow(n) on App options to override per-app.
const DefaultDeltaWindowCapacity = 64

// DeltaWindow is a per-actor circular buffer of state delta Updates. It
// is the mechanism backing the default projection.Store implementation:
// Cell pushes Update entries on every Version++ event, external
// subscribers query via Since (using the SinceVersion option) and the
// Store consults LatestVersion / OldestVersion to decide between
// live-only / replay-then-live / gap regimes per ARCHITECTURE.md §4.18.
//
// Version allocation is the caller's responsibility — Cell maintains a
// per-actor monotonic counter and stamps Updates before Push.
// DeltaWindow trusts that incoming Version values are strictly
// increasing within the lifetime of one actor; non-monotonic input is
// accepted but produces undefined Since-query ordering.
//
// Concurrency: writers (Cell goroutine) and readers (Watch / Store
// callers) are serialised by a single RWMutex — Push takes the write
// lock, Since / OldestVersion / LatestVersion take the read lock. The
// Window is small enough (default 64 entries) that lock-free designs
// are not worth the complexity at this tier.
//
// Parallel to events.Ring: same shape, version-keyed instead of
// SeqNo-keyed, capacity 64 instead of 256. The split mirrors the
// State Channel / Event Channel split in ARCHITECTURE.md §4.18 / §4.23.
type DeltaWindow struct {
	mu       sync.RWMutex
	buf      []Update
	capacity int
	head     int    // index of oldest Update in buf when count > 0
	count    int    // number of Updates held (0..capacity)
	latest   uint64 // Version of the most recently pushed Update (0 if empty)
	oldest   uint64 // Version of the oldest still-held Update (0 if empty)
}

// NewDeltaWindow returns an empty DeltaWindow with the given capacity.
// A capacity of 0 or negative substitutes DefaultDeltaWindowCapacity
// (64) so callers that forget to thread the WithProjectionDeltaWindow
// option still get a sane default rather than a degenerate zero-size
// buffer.
func NewDeltaWindow(capacity int) *DeltaWindow {
	if capacity <= 0 {
		capacity = DefaultDeltaWindowCapacity
	}
	return &DeltaWindow{
		buf:      make([]Update, capacity),
		capacity: capacity,
	}
}

// Capacity returns the maximum number of Updates the Window can hold
// before eviction. Stable for the lifetime of the Window. nil receiver
// returns 0 so Cell-side gap detection (`since < window.OldestVersion`)
// handles a never-populated actor without a panic.
func (w *DeltaWindow) Capacity() int {
	if w == nil {
		return 0
	}
	return w.capacity
}

// Push appends an Update at the tail. If the Window is full the oldest
// Update is evicted; OldestVersion advances to the new oldest. Version
// is taken from the input Update verbatim (DeltaWindow does not
// allocate IDs).
//
// Push is safe to call concurrently with itself only via the Cell
// serialisation guarantee — Cell goroutine is the sole writer. The
// internal lock is taken to make read paths safe against in-progress
// writes, not to make Push itself concurrency-safe.
func (w *DeltaWindow) Push(u Update) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.count < w.capacity {
		idx := (w.head + w.count) % w.capacity
		w.buf[idx] = u
		w.count++
		if w.count == 1 {
			w.oldest = u.Version
		}
	} else {
		// Full — overwrite at head, advance head.
		w.buf[w.head] = u
		w.head = (w.head + 1) % w.capacity
		w.oldest = w.buf[w.head].Version
	}
	w.latest = u.Version
}

// Since returns Updates with Version strictly greater than sinceVersion,
// in oldest-to-newest order.
//
// Filtering rules:
//   - sinceVersion: strict-greater-than. Pass 0 to get every Update
//     currently in the Window.
//   - limit: > 0 caps the result length; <= 0 returns every match.
//
// The result is a fresh slice owned by the caller — modifying it does
// not affect Window state. nil receiver returns nil so Watch on a
// never-populated actor returns an empty result without panicking.
//
// Gap detection is the caller's responsibility: when sinceVersion <
// OldestVersion() the requested range has been evicted; the caller
// (default projection.Store) should emit a gospore.projection.gap_too_large
// marker (UpdateGapTooLarge) and follow with a fresh full_snapshot.
//
// Note: unlike events.Ring.Since, DeltaWindow does not take a kinds
// filter — Update.Kind is determined by the Store at delivery time
// (delta vs full_snapshot vs gap_too_large), and the Window only
// stores deltas (UpdateDelta entries pushed by Cell on Version++).
// Filtering by Kind here would always return either everything or
// nothing.
func (w *DeltaWindow) Since(sinceVersion uint64, limit int) []Update {
	if w == nil {
		return nil
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.count == 0 {
		return nil
	}

	out := make([]Update, 0, w.count)
	for i := 0; i < w.count; i++ {
		idx := (w.head + i) % w.capacity
		u := w.buf[idx]
		if u.Version <= sinceVersion {
			continue
		}
		out = append(out, u)
		if limit > 0 && len(out) == limit {
			break
		}
	}
	return out
}

// OldestVersion returns the Version of the oldest Update currently held.
// Returns 0 when the Window is empty (callers must distinguish "empty"
// from "oldest happens to be 0" via LatestVersion or by tracking
// emptiness externally — a fresh actor never publishes Version 0
// because Cell allocates monotonically from 1).
func (w *DeltaWindow) OldestVersion() uint64 {
	if w == nil {
		return 0
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.oldest
}

// LatestVersion returns the Version of the most recently pushed Update.
// Returns 0 when the Window has never been pushed to. Used by the
// default Store to decide whether Watch(SinceVersion=N) should replay
// (N < latest), enter live mode straight away (N == latest), or emit
// a gap marker (N < oldest).
func (w *DeltaWindow) LatestVersion() uint64 {
	if w == nil {
		return 0
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.latest
}

// Len reports the number of Updates currently held. Useful for tests
// and debug/telemetry; the default Store uses Capacity / OldestVersion
// / LatestVersion for its actual decisions.
func (w *DeltaWindow) Len() int {
	if w == nil {
		return 0
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.count
}
