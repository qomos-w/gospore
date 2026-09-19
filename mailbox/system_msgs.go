package mailbox

import (
	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/ref"
)

// SystemMsg is the closed sum of system-lane payloads.
// Cells handle these directly; they never reach handlers.
type SystemMsg interface {
	isSystemMsg()
}

// Start triggers actor.OnStart. Sent once at spawn.
type Start struct{}

// Init triggers actor.OnInit. Sent before Start during the two-phase
// lifecycle: OnInit loads state, OnStart registers callables and starts
// goroutines.
type Init struct{}

// Stop triggers cascade-stop of the subtree, then actor.OnStop, then
// notification of watchers.
type Stop struct{}

// Idle transitions the actor to idle state: OnStop is called, background
// work should halt, but the actor remains in the tree and can still
// respond to callables (e.g. for inspection).
type Idle struct{}

// Destroy removes the actor from the tree. If the actor has not been
// idled yet, it is idled first. Then services are unregistered,
// watchers notified, and the mailbox closed.
type Destroy struct{}

// Suspend pauses user-lane consumption (system lane stays active).
type Suspend struct{}

// Resume re-enables user-lane consumption after Suspend.
type Resume struct{}

// Restart triggers cascade-stop, OnStop, factory rebuild, and OnStart.
// Reason carries the failure that motivated the restart (set by supervisor).
type Restart struct{ Reason error }

// Terminated is delivered to watchers when a watched target's OnStop
// has finished. If the receiving actor implements actor.Watcher, the
// cell invokes OnTerminated(ctx, Of).
type Terminated struct{ Of ref.Ref }

// Watch is sent to a target Cell when another actor calls ctx.Watch.
// The target adds Watcher to its watcher set so it will be notified
// when this Cell terminates.
type Watch struct {
	Watcher ref.Ref
	Kinds   []actor.WatchKind
}

// Unwatch is sent to a target Cell when another actor calls ctx.Unwatch.
// The target removes Watcher from its watcher set.
type Unwatch struct{ Watcher ref.Ref }

// Escalated is sent to a parent Cell when a child's supervisor returns
// Escalate. The parent's own supervisor is then consulted with the
// child's failure reason. If the parent also escalates, the escalation
// continues up the tree. At the root, escalation triggers App.Shutdown.
type Escalated struct {
	Child  ref.Ref
	Reason error
}

// RefreshProjection asks a Cell to recompute its component projection
// snapshot and publish it to the projection store. Used by the App-level
// gospore.projection.get callable when no prior subscriber has caused the
// snapshot to be materialised.
type RefreshProjection struct{}

func (Start) isSystemMsg()             {}
func (Init) isSystemMsg()              {}
func (Stop) isSystemMsg()              {}
func (Idle) isSystemMsg()              {}
func (Destroy) isSystemMsg()           {}
func (Suspend) isSystemMsg()           {}
func (Resume) isSystemMsg()            {}
func (Restart) isSystemMsg()           {}
func (Terminated) isSystemMsg()        {}
func (Watch) isSystemMsg()             {}
func (Unwatch) isSystemMsg()           {}
func (Escalated) isSystemMsg()         {}
func (RefreshProjection) isSystemMsg() {}
