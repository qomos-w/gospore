package invoke

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/qomos-w/gospore/message"
)

// ovFrameCap and ovByteCap bound a single slot's overflow queue — the FIFO
// of frames that arrived while the caller's 256-frame buffer was full.
// A consumer that lags transiently (lock contention, GC pause, scheduler
// hiccup) gets its frames preserved in overflow and pumped into the buffer
// as soon as it drains, instead of the previous behaviour of silently
// dropping frames after a 100ms grace and evicting the stream after eight
// consecutive full deliveries. Eviction now fires only when the consumer
// is so far behind (or gone) that the overflow itself blows past these
// caps. This also keeps the serial replyLoop of the owning Cell free of
// per-consumer wait budgets: Deliver never blocks, so one slow caller can
// no longer stall frame delivery for every other caller of the same actor
// (the 2026-09-12 cascade: three aggregator relay loops froze on one
// stalled agent's delivery grace and their own upstream streams were
// evicted within the same millisecond).
var (
	ovFrameCap = 8192
	ovByteCap  = 8 << 20
)

// maxPendingSlots is the per-table cap on simultaneously registered slots.
// Normal operation never approaches this (in-flight calls are bounded by
// mailbox capacity and caller concurrency); the cap exists so a caller
// leaking registrations (Register without a terminal frame and without
// Unregister) cannot grow the table without bound. When the cap is hit,
// the oldest slot (lowest CorID — the generator is monotonic, so lowest
// means registered first) is evicted and its caller unblocked with a
// terminal error, mirroring evictStalled. Each Cell owns its own table,
// so the blast radius of a leak is that Cell's calls only.
var maxPendingSlots = 4096

// recentEvictionsCap bounds how many recent eviction records the snapshot
// retains. Enough for attribution (which stream, when, why) without growing
// unbounded on a misbehaving caller.
const recentEvictionsCap = 8

// EvictionReason identifies why a pending slot was dropped without a terminal
// frame from the callee.
const (
	// EvictionStalled: the caller's buffer and overflow stayed full past
	// ovFrameCap/ovByteCap — the caller stopped reading without cancelling.
	EvictionStalled = "stalled"
	// EvictionCapacity: the table hit maxPendingSlots and evicted the oldest
	// slot to make room.
	EvictionCapacity = "capacity"
	// EvictionShutdown: the table's owning Cell was destroyed while calls
	// were still in flight — replies can no longer arrive, so every live
	// slot is terminated with an error to unblock the waiting Streams.
	EvictionShutdown = "shutdown"
)

// EvictionRecord attributes one eviction: which call (callID, CorID, mode),
// why, and when. Correlation IDs ride along so the record can be matched
// against the caller's own logs.
type EvictionRecord struct {
	CallID string    `json:"callId"`
	Mode   string    `json:"mode"`
	CorID  uint64    `json:"corId"`
	Reason string    `json:"reason"`
	Drops  int32     `json:"drops"`
	At     time.Time `json:"at"`
}

// PendingTable maps CorID to response channels for in-flight invocations.
// Thread-safe. Each Cell (or App) holds one instance to correlate Reply /
// Error / End frames back to the waiting Stream.
type PendingTable struct {
	mu    sync.Mutex
	slots map[uint64]*pendingSlot

	// Lifetime counters (monotonic) plus per-mode/per-call in-flight
	// breakdowns, surfaced via Snapshot for diagnostics (debug bundle /
	// gospore.cell.stats). Maintained under mu alongside the slot map.
	registered      atomic.Uint64
	completed       atomic.Uint64 // terminal KindEnd delivered
	errored         atomic.Uint64 // terminal KindError delivered
	sendFailed      atomic.Uint64 // unregistered because the send failed
	closedEarly     atomic.Uint64 // caller Closed/Cancelled before a terminal frame
	evictedStalled  atomic.Uint64 // evicted after persistent full-buffer stalls
	evictedCapacity atomic.Uint64 // evicted by the maxPendingSlots cap
	flushed         atomic.Uint64 // terminated by Flush (owning Cell destroyed)
	droppedFrames   atomic.Uint64 // frames dropped on a full caller buffer
	inFlightMode    map[string]int
	inFlightCall    map[string]int

	// recentEvictions is a capped ring (newest last) of eviction records
	// for attribution. Guarded by mu.
	recentEvictions []EvictionRecord
}

// pendingSlot tracks one in-flight call's buffer. callID/mode feed the
// in-flight breakdowns.
type pendingSlot struct {
	ch     chan message.Frame
	callID string
	mode   string

	// ovMu guards ov, the FIFO overflow of frames that arrived while ch was
	// full. The invariant "ov non-empty implies ch full" is maintained by
	// pumping ov into ch under ovMu after every append (Deliver) and after
	// every consumer receive (Stream.Recv via PendingTable.pumpFor), so
	// frame order is preserved and the consumer never parks while frames
	// are stranded in ov. ov is capped by ovFrameCap/ovByteCap; exceeding
	// the cap evicts the slot (evictStalled).
	ovMu    sync.Mutex
	ov      []message.Frame
	ovBytes int
}

// pumpLocked moves overflow frames into ch while it has room. The caller
// holds s.ovMu. Sends are non-blocking; when it returns, either ov is empty
// or ch is full.
func (s *pendingSlot) pumpLocked() {
	for len(s.ov) > 0 {
		select {
		case s.ch <- s.ov[0]:
			s.ovBytes -= len(s.ov[0].Body)
			s.ov = s.ov[1:]
		default:
			return
		}
	}
	if len(s.ov) == 0 {
		s.ov = nil
		s.ovBytes = 0
	}
}

// enqueue hands one frame to the slot: directly into ch when it has room
// AND nothing is queued in overflow (a direct send with a non-empty ov
// would jump the queued frames and violate FIFO), otherwise appended to
// the overflow FIFO and pumped. Returns false when the overflow blew past
// its caps — the caller must evict the slot.
func (s *pendingSlot) enqueue(frame message.Frame) bool {
	s.ovMu.Lock()
	defer s.ovMu.Unlock()
	if len(s.ov) == 0 {
		select {
		case s.ch <- frame:
			return true
		default:
		}
	}
	s.ov = append(s.ov, frame)
	s.ovBytes += len(frame.Body)
	if len(s.ov) > ovFrameCap || s.ovBytes > ovByteCap {
		return false
	}
	s.pumpLocked()
	return true
}

// InvokeStats is a point-in-time diagnostics snapshot of a PendingTable:
// in-flight invocations broken down by call mode (tell/unary/stream) and by
// callable ID, plus lifetime outcome counters. Each Cell owns one table
// (the root Cell's table doubles as the home for App/gateway-originated
// calls), so the snapshot reflects that actor's own pending registrations.
type InvokeStats struct {
	InFlight        int              `json:"inFlight"`
	InFlightByMode  map[string]int   `json:"inFlightByMode"`
	InFlightByCall  map[string]int   `json:"inFlightByCall"`
	Registered      uint64           `json:"registered"`
	Completed       uint64           `json:"completed"`
	Errored         uint64           `json:"errored"`
	SendFailed      uint64           `json:"sendFailed"`
	ClosedEarly     uint64           `json:"closedEarly"`
	EvictedStalled  uint64           `json:"evictedStalled"`
	EvictedCapacity uint64           `json:"evictedCapacity"`
	Flushed         uint64           `json:"flushed"`
	DroppedFrames   uint64           `json:"droppedFrames"`
	RecentEvictions []EvictionRecord `json:"recentEvictions"`
}

// NewPendingTable constructs an empty PendingTable.
func NewPendingTable() *PendingTable {
	return &PendingTable{
		slots:        make(map[uint64]*pendingSlot),
		inFlightMode: make(map[string]int),
		inFlightCall: make(map[string]int),
	}
}

// Register adds a slot for the given CorID. The caller owns the channel;
// Deliver sends frames into it. callID and mode (tell/unary/stream) feed the
// in-flight breakdowns in Snapshot; pass empty strings when unknown.
//
// If the table is at maxPendingSlots and corID is not already present,
// the oldest slot (lowest CorID) is evicted to make room: its CorID
// mapping is removed and a terminal error frame is pushed into its
// channel so that caller's Stream terminates with an error instead of
// hanging.
func (pt *PendingTable) Register(corID uint64, callID, mode string, ch chan message.Frame) {
	var victimCh chan message.Frame
	var victimID uint64
	var victim *pendingSlot
	pt.mu.Lock()
	_, exists := pt.slots[corID]
	if !exists && len(pt.slots) >= maxPendingSlots {
		for id, s := range pt.slots {
			if id < victimID || victimCh == nil {
				victimID, victimCh, victim = id, s.ch, s
			}
		}
		if victimCh != nil {
			pt.recordEvictionLocked(EvictionRecord{
				CallID: victim.callID,
				Mode:   victim.mode,
				CorID:  victimID,
				Reason: EvictionCapacity,
			})
			delete(pt.slots, victimID)
			pt.decInFlightLocked(victim)
			victim.ovMu.Lock()
			lost := len(victim.ov)
			victim.ov = nil
			victim.ovBytes = 0
			victim.ovMu.Unlock()
			pt.droppedFrames.Add(uint64(lost))
		}
	}
	if exists {
		if prev := pt.slots[corID]; prev != nil {
			pt.decInFlightLocked(prev)
		}
	}
	pt.slots[corID] = &pendingSlot{ch: ch, callID: callID, mode: mode}
	pt.incInFlightLocked(callID, mode)
	pt.mu.Unlock()
	pt.registered.Add(1)
	if victimCh != nil {
		pt.evictedCapacity.Add(1)
	}

	if victimCh != nil && victimID != corID {
		// Unblock the evicted caller. Never block here — the mapping is
		// gone, so no further frames can arrive for the victim slot.
		select {
		case <-victimCh:
		default:
		}
		select {
		case victimCh <- message.Frame{
			Kind:  message.KindError,
			CorID: victimID,
			Body:  []byte("invoke: pending table at capacity, oldest stream evicted"),
		}:
		default:
		}
	}
}

// Unregister removes the slot for the given CorID. No-op if not present.
// Counts as a closed-early termination when a live slot is removed (the
// caller Closed/Cancelled the Stream before a terminal frame arrived).
func (pt *PendingTable) Unregister(corID uint64) {
	pt.mu.Lock()
	s, ok := pt.slots[corID]
	if ok {
		delete(pt.slots, corID)
		pt.decInFlightLocked(s)
	}
	pt.mu.Unlock()
	if ok {
		pt.closedEarly.Add(1)
	}
}

// SendFailed removes the slot for a call whose frame could not be sent.
// Distinct from Unregister so failure counters separate transport failures
// from caller-side early closes.
func (pt *PendingTable) SendFailed(corID uint64) {
	pt.mu.Lock()
	s, ok := pt.slots[corID]
	if ok {
		delete(pt.slots, corID)
		pt.decInFlightLocked(s)
	}
	pt.mu.Unlock()
	if ok {
		pt.sendFailed.Add(1)
	}
}

// Flush terminates every live slot with a terminal KindError frame and
// empties the table. Called when the owning Cell is destroyed: its reply
// pipeline is gone, so replies can never arrive and waiting Streams must
// unblock with an error instead of hanging. Returns the number of slots
// that were terminated.
func (pt *PendingTable) Flush(reason string) int {
	pt.mu.Lock()
	type flushedSlot struct {
		corID uint64
		s     *pendingSlot
	}
	victims := make([]flushedSlot, 0, len(pt.slots))
	for corID, s := range pt.slots {
		victims = append(victims, flushedSlot{corID: corID, s: s})
		pt.recordEvictionLocked(EvictionRecord{
			CallID: s.callID,
			Mode:   s.mode,
			CorID:  corID,
			Reason: EvictionShutdown,
		})
	}
	pt.slots = make(map[uint64]*pendingSlot)
	pt.inFlightMode = make(map[string]int)
	pt.inFlightCall = make(map[string]int)
	pt.mu.Unlock()

	for _, v := range victims {
		pt.flushed.Add(1)
		// Drop anything parked in overflow — the reply pipeline is gone, so
		// those frames can never be pumped to the consumer anymore.
		v.s.ovMu.Lock()
		lost := len(v.s.ov)
		v.s.ov = nil
		v.s.ovBytes = 0
		v.s.ovMu.Unlock()
		pt.droppedFrames.Add(uint64(lost))
		// Make room for the terminal frame by dropping the oldest queued
		// one, then push the error without blocking — the mapping is gone,
		// so no further frames can arrive for this slot.
		select {
		case <-v.s.ch:
		default:
		}
		select {
		case v.s.ch <- message.Frame{
			Kind:  message.KindError,
			CorID: v.corID,
			Body:  []byte("invoke: pending table flushed: " + reason),
		}:
		default:
		}
	}
	return len(victims)
}

// incInFlightLocked/decInFlightLocked maintain the per-mode/per-callID
// in-flight breakdowns; callers must hold pt.mu.
func (pt *PendingTable) incInFlightLocked(callID, mode string) {
	if mode != "" {
		pt.inFlightMode[mode]++
	}
	if callID != "" {
		pt.inFlightCall[callID]++
	}
}

func (pt *PendingTable) decInFlightLocked(s *pendingSlot) {
	if s == nil {
		return
	}
	if s.mode != "" {
		if n := pt.inFlightMode[s.mode]; n > 1 {
			pt.inFlightMode[s.mode] = n - 1
		} else {
			delete(pt.inFlightMode, s.mode)
		}
	}
	if s.callID != "" {
		if n := pt.inFlightCall[s.callID]; n > 1 {
			pt.inFlightCall[s.callID] = n - 1
		} else {
			delete(pt.inFlightCall, s.callID)
		}
	}
}

// recordEvictionLocked appends an attribution record to the capped ring
// (newest last). Caller must hold pt.mu.
func (pt *PendingTable) recordEvictionLocked(rec EvictionRecord) {
	rec.At = time.Now()
	pt.recentEvictions = append(pt.recentEvictions, rec)
	if n := len(pt.recentEvictions); n > recentEvictionsCap {
		pt.recentEvictions = append(pt.recentEvictions[:0], pt.recentEvictions[n-recentEvictionsCap:]...)
	}
}

// Snapshot returns a point-in-time diagnostics copy of the table's
// in-flight breakdowns and lifetime outcome counters.
func (pt *PendingTable) Snapshot() InvokeStats {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	stats := InvokeStats{
		InFlight:        len(pt.slots),
		InFlightByMode:  make(map[string]int, len(pt.inFlightMode)),
		InFlightByCall:  make(map[string]int, len(pt.inFlightCall)),
		Registered:      pt.registered.Load(),
		Completed:       pt.completed.Load(),
		Errored:         pt.errored.Load(),
		SendFailed:      pt.sendFailed.Load(),
		ClosedEarly:     pt.closedEarly.Load(),
		EvictedStalled:  pt.evictedStalled.Load(),
		EvictedCapacity: pt.evictedCapacity.Load(),
		Flushed:         pt.flushed.Load(),
		DroppedFrames:   pt.droppedFrames.Load(),
	}
	if n := len(pt.recentEvictions); n > 0 {
		stats.RecentEvictions = make([]EvictionRecord, n)
		copy(stats.RecentEvictions, pt.recentEvictions)
	}
	for k, v := range pt.inFlightMode {
		stats.InFlightByMode[k] = v
	}
	for k, v := range pt.inFlightCall {
		stats.InFlightByCall[k] = v
	}
	return stats
}

// Deliver routes a frame to the slot registered for frame.CorID.
// Returns true if the frame was enqueued (buffer or overflow), false if no
// slot was found or the slot's overflow blew past its caps (in which case
// the slot has been evicted).
//
// Deliver never blocks: the replyLoop goroutine of the owning Cell calls it
// serially for every streaming reply, so any per-consumer wait budget here
// would stall delivery for ALL callers behind it. A full caller buffer
// parks the frame in the slot's overflow FIFO instead; the consumer's
// Stream.Recv pumps the overflow back into the buffer as it drains, so a
// transiently stalled consumer neither loses frames nor gets its stream
// terminated.
//
// Terminal frames (KindEnd / KindError) auto-release the slot after
// enqueue: every call — tell, unary, or streaming — is terminated by
// one of these, so the pending slot's lifetime matches the call instead
// of relying on callers to Close each Stream. The buffered channel stays
// alive (held by the caller's Stream) so already-enqueued Reply frames
// remain readable; only the CorID mapping is removed.
func (pt *PendingTable) Deliver(frame message.Frame) bool {
	pt.mu.Lock()
	s, ok := pt.slots[frame.CorID]
	pt.mu.Unlock()
	if !ok {
		return false
	}
	if !s.enqueue(frame) {
		pt.evictStalled(frame.CorID, s)
		return false
	}
	switch frame.Kind {
	case message.KindEnd, message.KindError:
		if frame.Kind == message.KindEnd {
			pt.completed.Add(1)
		} else {
			pt.errored.Add(1)
		}
		pt.mu.Lock()
		delete(pt.slots, frame.CorID)
		pt.decInFlightLocked(s)
		pt.mu.Unlock()
	}
	return true
}

// pumpFor captures the slot registered for corID and returns a closure that
// pumps its overflow into the buffer. streamImpl calls the closure after
// every successful receive so frames parked behind a full buffer are
// surfaced as soon as the consumer makes room. The slot is captured at
// stream construction (Register always precedes NewStream): the mapping is
// removed when the terminal frame is merely enqueued into overflow, so a
// live lookup would strand frames behind the terminal — the slot object,
// like ch, outlives the mapping.
func (pt *PendingTable) pumpFor(corID uint64) func() {
	pt.mu.Lock()
	s, ok := pt.slots[corID]
	pt.mu.Unlock()
	if !ok {
		return func() {}
	}
	return func() {
		s.ovMu.Lock()
		s.pumpLocked()
		s.ovMu.Unlock()
	}
}

// evictStalled removes a persistently stalled slot and unblocks its caller.
// The slot pointer is re-checked under the lock so a concurrent
// Unregister/Register for the same CorID is never clobbered.
func (pt *PendingTable) evictStalled(corID uint64, s *pendingSlot) {
	pt.mu.Lock()
	cur, ok := pt.slots[corID]
	if !ok || cur != s {
		pt.mu.Unlock()
		return
	}
	delete(pt.slots, corID)
	pt.decInFlightLocked(s)
	s.ovMu.Lock()
	lost := int32(len(s.ov)) + 1
	s.ov = nil
	s.ovBytes = 0
	s.ovMu.Unlock()
	pt.recordEvictionLocked(EvictionRecord{
		CallID: s.callID,
		Mode:   s.mode,
		CorID:  corID,
		Reason: EvictionStalled,
		Drops:  lost,
	})
	pt.mu.Unlock()
	pt.evictedStalled.Add(1)
	pt.droppedFrames.Add(uint64(lost))

	// Make room for the terminal frame by dropping the oldest queued one,
	// then push the error. A racing Deliver could refill the freed slot
	// after Unregister — never block here (the mapping is gone, so no
	// further frames can arrive for this slot).
	select {
	case <-s.ch:
	default:
	}
	select {
	case s.ch <- message.Frame{
		Kind:  message.KindError,
		CorID: corID,
		Body:  []byte("invoke: caller stalled (buffer full), stream evicted"),
	}:
	default:
	}
}

// Has reports whether a slot exists for the given CorID.
func (pt *PendingTable) Has(corID uint64) bool {
	pt.mu.Lock()
	_, ok := pt.slots[corID]
	pt.mu.Unlock()
	return ok
}

// Len returns the number of registered slots.
func (pt *PendingTable) Len() int {
	pt.mu.Lock()
	n := len(pt.slots)
	pt.mu.Unlock()
	return n
}
