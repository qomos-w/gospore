package id

import (
	"sync/atomic"

	"github.com/qomos-w/spore/identity"
)

// Canonical is the App-process-global generator for 128-bit canonical IDs.
// It holds (runtimeSlot, incarnation) constants, a clock function, and an
// atomic sequence counter. Constructed once by App; shared by every Cell
// and exposed on actor.Context as NewID for runtime-issued identifiers
// (actor IDs, request IDs, event IDs, frame IDs, ...). Concurrency-safe.
type Canonical struct {
	slot        uint16
	incarnation uint16
	seq         atomic.Uint64
	clock       func() uint64
}

// NewCanonical constructs a Canonical ID generator.
// slot identifies the runtime instance (assigned at deploy; fixed within
// a process). incarnation increments on same-slot restart so old-process
// IDs cannot collide with new-process IDs even at adjacent timestamps.
// clock returns the current time as milliseconds since epoch (production:
// time.Now().UnixMilli; tests: a fake clock).
func NewCanonical(slot uint16, incarnation uint16, clock func() uint64) *Canonical {
	return &Canonical{
		slot:        slot,
		incarnation: incarnation,
		clock:       clock,
	}
}

// Next produces the next unique 128-bit canonical ID.
// ts comes from clock(); seq increments atomically.
// IDs within the same millisecond are ordered by seq; across milliseconds
// by ts. Concurrency-safe.
//
// Panics on spore overflow (timestamp > 2^48-1 = year 10889 absurd;
// sequence > 2^48-1 = 281T IDs absurd) — Next has no error return.
func (g *Canonical) Next() ActorID {
	seq := g.seq.Add(1)
	ts := g.clock()
	cid, err := identity.NewCanonicalID(ts, g.slot, g.incarnation, seq)
	if err != nil {
		panic(err)
	}
	return From(cid)
}
