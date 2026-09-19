package id

import "sync/atomic"

// CorID is the per-Frame correlation identifier (App-globally unique,
// monotonic uint64). Lifecycle: from the Invoke that opens a call to the
// terminal End/Error/Cancel frame. Unlike ActorID, CorID does not carry
// timestamp/slot/incarnation — it is a flat increment.
type CorID uint64

// CorIDGenerator produces unique CorIDs for an App process. Concurrency-safe.
type CorIDGenerator struct {
	seq atomic.Uint64
}

// NewCorIDGenerator constructs a CorID generator. The first Next() yields 1.
func NewCorIDGenerator() *CorIDGenerator {
	return &CorIDGenerator{}
}

// Next returns the next unique CorID.
func (g *CorIDGenerator) Next() CorID {
	return CorID(g.seq.Add(1))
}
