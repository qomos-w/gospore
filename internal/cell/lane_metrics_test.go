package cell

import (
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/internal/handler"
	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/message"
)

// newTimedCell builds a minimal cell whose handler table is non-nil so
// calls can be registered and dispatched without the full App stack.
func newTimedCell(t *testing.T) *Cell {
	t.Helper()
	return New(Config{
		Namespace: "test-ns",
		EventBus:  NewEventBus(),
		Handlers:  handler.NewTable(),
	})
}

func (c *Cell) startOwnerLoopForTest() {
	c.ownerWG.Add(1)
	go c.ownerLoop()
}

func (c *Cell) stopOwnerLoopForTest() {
	c.ownerQMu.Lock()
	q := c.ownerQ
	c.ownerQ = nil
	c.ownerQClosed = true
	c.ownerQMu.Unlock()
	if q != nil {
		close(q)
	}
	c.ownerWG.Wait()
}

func callEnv(callID string) mailbox.Envelope {
	return mailbox.Envelope{Frame: message.Frame{Kind: message.KindCall, CallID: callID}}
}

func ownerLaneStats(t *testing.T, c *Cell) LaneRuntimeStats {
	t.Helper()
	for _, l := range c.Stats().Lanes {
		if l.Name == laneNameOwner {
			return l
		}
	}
	t.Fatal("owner lane missing from stats")
	return LaneRuntimeStats{}
}

// TestLaneMetrics_RingKeepsRecentOrder verifies the timing ring records the
// last laneTimingRingSize entries in oldest-to-newest order.
func TestLaneMetrics_RingKeepsRecentOrder(t *testing.T) {
	m := &laneMetrics{}
	for i := 0; i < laneTimingRingSize+3; i++ {
		m.record(LaneTiming{CallID: "c", WaitNs: uint64(i), ExecNs: uint64(i)})
	}
	st := m.snapshot("x", 0, 0)
	if len(st.Recent) != laneTimingRingSize {
		t.Fatalf("Recent len = %d, want %d", len(st.Recent), laneTimingRingSize)
	}
	// After 19 records of 0..18, the ring holds 3..18; first element must be
	// the oldest surviving record.
	if st.Recent[0].WaitNs != 3 {
		t.Fatalf("oldest WaitNs = %d, want 3", st.Recent[0].WaitNs)
	}
	if st.Recent[len(st.Recent)-1].WaitNs != laneTimingRingSize+2 {
		t.Fatalf("newest WaitNs = %d, want %d", st.Recent[len(st.Recent)-1].WaitNs, laneTimingRingSize+2)
	}
	if st.Timing.Count != laneTimingRingSize {
		t.Fatalf("Count = %d, want %d", st.Timing.Count, laneTimingRingSize)
	}
	if st.Timing.WaitMaxNs != laneTimingRingSize+2 {
		t.Fatalf("WaitMaxNs = %d, want %d", st.Timing.WaitMaxNs, laneTimingRingSize+2)
	}
}

// TestSummarizeLaneTimings_Percentiles verifies nearest-rank percentile
// computation over known values.
func TestSummarizeLaneTimings_Percentiles(t *testing.T) {
	recs := []LaneTiming{
		{WaitNs: 10, ExecNs: 100},
		{WaitNs: 20, ExecNs: 200},
		{WaitNs: 30, ExecNs: 300},
		{WaitNs: 40, ExecNs: 400},
	}
	s := summarizeLaneTimings(recs)
	if s.Count != 4 {
		t.Fatalf("Count = %d, want 4", s.Count)
	}
	// nearest-rank p50 of 4 samples -> 2nd value; p99 -> 4th value.
	if s.WaitP50Ns != 20 || s.WaitP99Ns != 40 || s.WaitMaxNs != 40 {
		t.Fatalf("wait percentiles = %d/%d/%d, want 20/40/40", s.WaitP50Ns, s.WaitP99Ns, s.WaitMaxNs)
	}
	if s.ExecP50Ns != 200 || s.ExecP99Ns != 400 || s.ExecMaxNs != 400 {
		t.Fatalf("exec percentiles = %d/%d/%d, want 200/400/400", s.ExecP50Ns, s.ExecP99Ns, s.ExecMaxNs)
	}
}

// TestTruncateCallID verifies oversized call IDs are bounded before entering
// a timing ring (log/field truncation rule).
func TestTruncateCallID(t *testing.T) {
	short := "wiki.page.get"
	if got := truncateCallID(short); got != short {
		t.Fatalf("truncateCallID(%q) = %q", short, got)
	}
	long := make([]byte, laneTimingCallIDMax+50)
	for i := range long {
		long[i] = 'x'
	}
	got := truncateCallID(string(long))
	if len(got) != laneTimingCallIDMax {
		t.Fatalf("len = %d, want %d", len(got), laneTimingCallIDMax)
	}
}

// TestLaneTiming_SlowHandlerQueuedWaitRecorded is the acceptance test for
// per-(lane, callID) queue-wait observability: a slow stateful handler on
// the owner lane forces a second call to queue behind it; the second call's
// ring entry must carry the accrued wait, and the busy window must be
// visible while the slow handler runs.
func TestLaneTiming_SlowHandlerQueuedWaitRecorded(t *testing.T) {
	c := newTimedCell(t)
	ctx := NewStartContextForTest(c)

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	if err := ctx.Register("test.slow", func(actor.Context) error {
		entered <- struct{}{}
		<-release
		return nil
	}); err != nil {
		t.Fatalf("register slow: %v", err)
	}
	if err := ctx.Register("test.quick", func(actor.Context) error { return nil }); err != nil {
		t.Fatalf("register quick: %v", err)
	}

	c.startOwnerLoopForTest()
	defer c.stopOwnerLoopForTest()

	if err := c.Recv(callEnv("test.slow")); err != nil {
		t.Fatalf("Recv slow: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("slow handler did not start")
	}

	// The queued call must wait behind the slow handler.
	if err := c.Recv(callEnv("test.quick")); err != nil {
		t.Fatalf("Recv quick: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// While the slow handler runs, the owner lane must report busy.
	if st := ownerLaneStats(t, c); !st.Busy || st.BusyForNs == 0 {
		t.Fatalf("owner lane busy = %v busyForNs = %d, want true/>0 while slow handler runs", st.Busy, st.BusyForNs)
	}

	close(release)

	// Wait until both executions are recorded in the owner lane ring.
	deadline := time.Now().Add(2 * time.Second)
	var st LaneRuntimeStats
	for {
		st = ownerLaneStats(t, c)
		if st.Timing.Count >= 2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if st.Timing.Count < 2 {
		t.Fatalf("owner lane timing count = %d, want >= 2", st.Timing.Count)
	}

	var slowExec, quickWait uint64
	for _, r := range st.Recent {
		switch r.CallID {
		case "test.slow":
			slowExec = r.ExecNs
		case "test.quick":
			quickWait = r.WaitNs
		}
	}
	// The slow handler blocked for >= 50ms after quick was queued, so
	// quick's wait must cover most of it (margin for scheduler jitter).
	if quickWait < uint64(20*time.Millisecond) {
		t.Fatalf("queued call waitNs = %d, want >= %d", quickWait, uint64(20*time.Millisecond))
	}
	if slowExec < uint64(40*time.Millisecond) {
		t.Fatalf("slow handler execNs = %d, want >= %d", slowExec, uint64(40*time.Millisecond))
	}
	if st.Timing.WaitMaxNs < quickWait || st.Timing.ExecMaxNs < slowExec {
		t.Fatal("summary maxima do not cover the observed records")
	}
	// After both handlers completed the lane must be idle again.
	if st.Busy {
		t.Fatal("owner lane still busy after handlers completed")
	}
}

// TestLaneTiming_CustomLaneWaitRecorded mirrors the owner-lane test for a
// custom RegisterLoop lane: wait/exec must be attributed to the custom lane
// name, not the owner lane.
func TestLaneTiming_CustomLaneWaitRecorded(t *testing.T) {
	c := newTimedCell(t)
	ctx := NewStartContextForTest(c)
	if err := ctx.RegisterLoop("custom.exec", actor.ModeStateful); err != nil {
		t.Fatalf("RegisterLoop: %v", err)
	}

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	if err := ctx.Register("test.lane.slow", func(actor.Context) error {
		entered <- struct{}{}
		<-release
		return nil
	}, actor.WithLoop("custom.exec")); err != nil {
		t.Fatalf("register lane slow: %v", err)
	}
	if err := ctx.Register("test.lane.quick", func(actor.Context) error { return nil }, actor.WithLoop("custom.exec")); err != nil {
		t.Fatalf("register lane quick: %v", err)
	}

	if err := c.Recv(callEnv("test.lane.slow")); err != nil {
		t.Fatalf("Recv lane slow: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("custom lane slow handler did not start")
	}
	if err := c.Recv(callEnv("test.lane.quick")); err != nil {
		t.Fatalf("Recv lane quick: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	close(release)

	deadline := time.Now().Add(2 * time.Second)
	var lane LaneRuntimeStats
	for {
		for _, l := range c.Stats().Lanes {
			if l.Name == "custom.exec" {
				lane = l
			}
		}
		if lane.Timing.Count >= 2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if lane.Name != "custom.exec" {
		t.Fatal("custom.exec lane missing from stats")
	}
	if lane.Timing.Count < 2 {
		t.Fatalf("custom lane timing count = %d, want >= 2", lane.Timing.Count)
	}
	var quickWait uint64
	for _, r := range lane.Recent {
		if r.CallID == "test.lane.quick" {
			quickWait = r.WaitNs
		}
	}
	if quickWait < uint64(20*time.Millisecond) {
		t.Fatalf("custom lane queued waitNs = %d, want >= %d", quickWait, uint64(20*time.Millisecond))
	}
}

// TestLaneSnapshots_AlwaysIncludesBuiltinLanes verifies the additive Lanes
// slice exposes the three builtin ingress lanes with their capacities even
// before any traffic.
func TestLaneSnapshots_AlwaysIncludesBuiltinLanes(t *testing.T) {
	c := newTimedCell(t)
	lanes := c.Stats().Lanes
	want := map[string]int{
		laneNameOwner:  ownerQueueCapacity,
		laneNameSystem: 64,
		laneNameReply:  64,
	}
	seen := map[string]LaneRuntimeStats{}
	for _, l := range lanes {
		seen[l.Name] = l
	}
	for name, capc := range want {
		l, ok := seen[name]
		if !ok {
			t.Fatalf("lane %q missing from stats", name)
		}
		if l.Capacity != capc {
			t.Fatalf("lane %q capacity = %d, want %d", name, l.Capacity, capc)
		}
		if l.HighWater != 0 {
			t.Fatalf("lane %q highWater = %d, want 0 before traffic", name, l.HighWater)
		}
	}
	// Names must be sorted for stable consumption.
	for i := 1; i < len(lanes); i++ {
		if lanes[i-1].Name >= lanes[i].Name {
			t.Fatalf("lanes not sorted: %q >= %q", lanes[i-1].Name, lanes[i].Name)
		}
	}
}
