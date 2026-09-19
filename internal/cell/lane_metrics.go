package cell

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/qomos-w/gospore/internal/handler"
	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/message"
)

// This file implements per-lane observability for the cell runtime: queue
// wait and handler execution durations per (lane, callID), queue-depth
// high-water marks, and busy windows for single-goroutine lanes. It is the
// companion to the owner-lane no-blocking constraint — per-(lane, callID)
// execution and queue-wait durations must be observable via Cell.Stats().

// Canonical lane names used by the per-lane metrics registry. Custom loops
// registered via RegisterLoop appear under their own name.
const (
	laneNameOwner  = "owner"
	laneNameSystem = "system"
	laneNameReply  = "reply"
	laneNamePure   = "pure"
)

// laneTimingRingSize bounds the per-lane ring of recent handler-execution
// records. 16 keeps snapshot cost trivial and prevents memory growth on hot
// lanes (the ring is a fixed-size array, never reallocated).
const laneTimingRingSize = 16

// laneTimingCallIDMax bounds a call ID recorded into a lane timing ring so
// no oversized identifier can bloat stats snapshots (log/field truncation
// rule).
const laneTimingCallIDMax = 128

// LaneTiming is one completed (lane, callID) execution observation: how long
// the call waited in the lane queue and how long the handler executed.
type LaneTiming struct {
	CallID string    // truncated to laneTimingCallIDMax
	WaitNs uint64    // queue wait: enqueue stamp to dispatch start
	ExecNs uint64    // handler execution duration
	DoneAt time.Time // completion time
}

// laneMetrics accumulates per-lane observability for one ingress lane:
// queue-depth high-water, the current busy window (handler start time) for
// single-goroutine lanes, and a bounded ring of recent completed executions
// with wait/exec durations. All access is mutex-guarded: lanes run on their
// own goroutines while Stats() may be called from any goroutine.
type laneMetrics struct {
	mu        sync.Mutex
	ring      [laneTimingRingSize]LaneTiming
	next      int       // next ring write index
	n         int       // records held (capped at laneTimingRingSize)
	highWater int       // max queue depth observed since cell start
	busySince time.Time // zero = lane idle
	noBusy    bool      // true for concurrent lanes (pure) where busy-since is meaningless
}

// noteDepth records one queue-depth observation, raising the high-water mark
// when the lane has never been this deep.
func (m *laneMetrics) noteDepth(depth int) {
	if depth <= 0 {
		return
	}
	m.mu.Lock()
	if depth > m.highWater {
		m.highWater = depth
	}
	m.mu.Unlock()
}

// setBusy marks the lane's single-goroutine handler window as started.
func (m *laneMetrics) setBusy() {
	if m.noBusy {
		return
	}
	m.mu.Lock()
	m.busySince = time.Now()
	m.mu.Unlock()
}

// clearBusy marks the lane idle again.
func (m *laneMetrics) clearBusy() {
	if m.noBusy {
		return
	}
	m.mu.Lock()
	m.busySince = time.Time{}
	m.mu.Unlock()
}

// record appends one completed execution to the ring, overwriting the oldest
// entry once full.
func (m *laneMetrics) record(t LaneTiming) {
	m.mu.Lock()
	m.ring[m.next] = t
	m.next = (m.next + 1) % laneTimingRingSize
	if m.n < laneTimingRingSize {
		m.n++
	}
	m.mu.Unlock()
}

// LaneRuntimeStats is the per-lane observability snapshot: queue depth and
// capacity, the depth high-water, the busy window (handler start time) for
// single-goroutine lanes, and the recent timing ring summary.
type LaneRuntimeStats struct {
	Name      string
	Depth     int
	Capacity  int
	HighWater int
	Busy      bool
	BusySince time.Time
	BusyForNs uint64
	Timing    LaneTimingSummary
	Recent    []LaneTiming // oldest to newest, bounded by laneTimingRingSize
}

// LaneTimingSummary is a percentile/max summary over a lane's recent ring of
// handler executions.
type LaneTimingSummary struct {
	Count     int
	WaitP50Ns uint64
	WaitP99Ns uint64
	WaitMaxNs uint64
	ExecP50Ns uint64
	ExecP99Ns uint64
	ExecMaxNs uint64
}

// snapshot returns the lane's observability state paired with live queue
// depth/capacity. Recent is ordered oldest to newest.
func (m *laneMetrics) snapshot(name string, depth, capacity int) LaneRuntimeStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := LaneRuntimeStats{
		Name:      name,
		Depth:     depth,
		Capacity:  capacity,
		HighWater: m.highWater,
	}
	if !m.busySince.IsZero() {
		st.Busy = true
		st.BusySince = m.busySince
		st.BusyForNs = uint64(time.Since(m.busySince))
	}
	n := m.n
	if n > laneTimingRingSize {
		n = laneTimingRingSize
	}
	var recs []LaneTiming
	if n > 0 {
		recs = make([]LaneTiming, 0, n)
		for i := 0; i < n; i++ {
			idx := (m.next - n + i + laneTimingRingSize) % laneTimingRingSize
			recs = append(recs, m.ring[idx])
		}
	}
	st.Recent = recs
	st.Timing = summarizeLaneTimings(recs)
	return st
}

// summarizeLaneTimings computes nearest-rank p50/p99 and max over the wait
// and exec durations of a lane's recent ring. With few samples the
// percentiles collapse toward the observed values; acceptable for
// diagnostics.
func summarizeLaneTimings(recs []LaneTiming) LaneTimingSummary {
	var s LaneTimingSummary
	if len(recs) == 0 {
		return s
	}
	waits := make([]uint64, len(recs))
	execs := make([]uint64, len(recs))
	for i, r := range recs {
		waits[i] = r.WaitNs
		execs[i] = r.ExecNs
	}
	sort.Slice(waits, func(i, j int) bool { return waits[i] < waits[j] })
	sort.Slice(execs, func(i, j int) bool { return execs[i] < execs[j] })
	s.Count = len(recs)
	s.WaitP50Ns = pctRank(waits, 0.50)
	s.WaitP99Ns = pctRank(waits, 0.99)
	s.WaitMaxNs = waits[len(waits)-1]
	s.ExecP50Ns = pctRank(execs, 0.50)
	s.ExecP99Ns = pctRank(execs, 0.99)
	s.ExecMaxNs = execs[len(execs)-1]
	return s
}

// pctRank returns the nearest-rank percentile of an ascending-sorted slice.
func pctRank(sorted []uint64, p float64) uint64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// laneSnapshots assembles the per-lane observability slice: the three
// builtin ingress lanes, every registered custom loop, and the pure lane
// once it has executed at least one call. Sorted by lane name for stable
// consumption.
func (c *Cell) laneSnapshots() []LaneRuntimeStats {
	lanes := make([]LaneRuntimeStats, 0, 4)
	add := func(name string, depth, capc int) {
		lanes = append(lanes, c.laneMetricsFor(name).snapshot(name, depth, capc))
	}
	add(laneNameOwner, len(c.ownerQ), cap(c.ownerQ))
	add(laneNameSystem, len(c.systemQ), cap(c.systemQ))
	add(laneNameReply, len(c.replyQ), cap(c.replyQ))

	c.loopsMu.Lock()
	custom := make([]string, 0, len(c.loops))
	for name := range c.loops {
		custom = append(custom, name)
	}
	c.loopsMu.Unlock()
	for _, name := range custom {
		c.loopsMu.Lock()
		lane := c.loops[name]
		c.loopsMu.Unlock()
		depth, capacity := 0, 0
		if lane != nil && lane.q != nil {
			depth, capacity = len(lane.q), cap(lane.q)
		}
		add(name, depth, capacity)
	}

	c.lanesMu.RLock()
	_, pureUsed := c.laneStats[laneNamePure]
	c.lanesMu.RUnlock()
	if pureUsed {
		add(laneNamePure, 0, 0)
	}

	sort.Slice(lanes, func(i, j int) bool { return lanes[i].Name < lanes[j].Name })
	return lanes
}

// laneMetricsFor returns the metrics record for a lane, creating it on first
// use. The pure lane is flagged noBusy because its handlers run on many
// concurrent goroutines — a busy window would be meaningless there.
func (c *Cell) laneMetricsFor(name string) *laneMetrics {
	c.lanesMu.RLock()
	m, ok := c.laneStats[name]
	c.lanesMu.RUnlock()
	if ok {
		return m
	}
	c.lanesMu.Lock()
	defer c.lanesMu.Unlock()
	if existing, ok := c.laneStats[name]; ok {
		return existing
	}
	m = &laneMetrics{noBusy: name == laneNamePure}
	c.laneStats[name] = m
	return m
}

// noteLaneDepth records one queue-depth observation for high-water tracking.
func (c *Cell) noteLaneDepth(name string, depth int) {
	if depth <= 0 {
		return
	}
	c.laneMetricsFor(name).noteDepth(depth)
}

// timedInvoke wraps invokeCall with per-lane timing observability: the
// queue wait (from the envelope's EnqueuedAt stamp), the busy window for
// single-goroutine lanes, and one ring record (callID, waitNs, execNs)
// per completed execution. Panics inside invokeCall are recovered there,
// so the deferred record always runs.
func (c *Cell) timedInvoke(lane string, inv *handler.Invoker, env mailbox.Envelope, reply func(message.Frame)) {
	m := c.laneMetricsFor(lane)
	var waitNs uint64
	if !env.EnqueuedAt.IsZero() {
		waitNs = uint64(time.Since(env.EnqueuedAt))
	}
	m.setBusy()
	start := time.Now()
	defer func() {
		m.record(LaneTiming{
			CallID: truncateCallID(env.Frame.CallID),
			WaitNs: waitNs,
			ExecNs: uint64(time.Since(start)),
			DoneAt: time.Now(),
		})
		m.clearBusy()
	}()
	c.invokeCall(inv, env, reply)
}

// withLaneBusy marks a single-goroutine lane busy for the duration of fn so
// a stuck system/reply handler is visible in stats snapshots.
func (c *Cell) withLaneBusy(name string, fn func()) {
	m := c.laneMetricsFor(name)
	m.setBusy()
	defer m.clearBusy()
	fn()
}

// truncateCallID bounds call IDs recorded into lane timing rings so no
// oversized identifier can bloat stats snapshots (log/field truncation
// rule).
func truncateCallID(s string) string {
	if len(s) <= laneTimingCallIDMax {
		return s
	}
	return s[:laneTimingCallIDMax-3] + "..."
}
