// Package events exposes the Event Channel: a per-actor stream of
// invocation, lifecycle, and child-tracking event Records.
//
// Events and Watch share the same underlying event source; they are
// the same vocabulary (actor.WatchKind) routed to two consumers:
//   - Watch dispatches into actor mailboxes for in-process handlers.
//   - Events buffers Records in a ring (default 256) and pushes them
//     over actor.Subscription[Record] for external observers.
//
// Records carry resolved visibility explicitly so external observers can
// apply the same access policy the runtime used when emitting them.
package events

import (
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
)

// Record is one event entry in an actor's event ring.
//
// Different Kinds populate different subsets of the optional fields:
//
//	WatchInvokeInStarted   — Caller, CallID, SchemaNS, SchemaID
//	WatchInvokeInEnded     — Caller, CallID, Duration, ErrorMsg
//	WatchInvokeInPanicked  — CallID, Reason
//	WatchInvokeOutStarted  — Target, CallID, CorID
//	WatchInvokeOutEnded    — Target, CallID, CorID, Duration, ErrorMsg
//	WatchInvokeOutCancelled — Target, CallID, CorID, Reason
//	WatchTerminated        — Reason (Reason field, not ErrorMsg)
//	WatchStarted           — (no extra fields)
//	WatchRestarted         — Reason
//	WatchChildSpawned      — ChildID
//	WatchChildTerminated   — ChildID, Reason
type Record struct {
	SeqNo      uint64
	Kind       actor.WatchKind
	// PolicyScope is derived from the CallID or actor path for policy evaluation.
	Timestamp  time.Time
	ActorID    id.ActorID

	Caller   id.ActorID
	Target   id.ActorID
	CallID   string
	SchemaNS string
	SchemaID uint32
	CorID    uint64
	Duration time.Duration
	ErrorMsg string
	Reason   string
	ChildID  id.ActorID
}

// Store is the App-wide events lookup and subscription surface.
type Store interface {
	// Recent returns up to limit Records for actorID with SeqNo >
	// sinceSeqNo, optionally filtered to kinds (empty = all kinds).
	// Order: oldest to newest.
	Recent(actorID id.ActorID, sinceSeqNo uint64, kinds []actor.WatchKind, limit int) []Record

	// Subscribe returns a long-lived stream. If sinceSeqNo > 0 the
	// stream first replays missing Records from the ring (or signals
	// gap_too_large via Recent shaping in the implementation), then
	// transitions to live delivery.
	Subscribe(actorID id.ActorID, sinceSeqNo uint64, kinds []actor.WatchKind) (actor.Subscription[Record], error)

	// HasSubscribers is the Tier-0 hint — when no one is subscribed
	// for an actor, the Cell skips Record allocation entirely.
	HasSubscribers(actorID id.ActorID) bool
}
