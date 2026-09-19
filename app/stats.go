package app

import (
	"fmt"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/internal/cell"
	"github.com/qomos-w/gospore/invoke"
)

const cellStatsCallID = "gospore.cell.stats"

// cellStatsReq resolves a target actor by path (canonical ULID, service name,
// or actor type — same resolution as gospore.projection.get).
type cellStatsReq struct {
	ActorPath string `json:"actorPath"`
}

// QueueStats is the depth/capacity pair for one ingress lane.
type QueueStats struct {
	Depth    int `json:"depth"`
	Capacity int `json:"capacity"`
}

// LaneStats is the per-lane observability record in the cell stats
// transport shape: queue depth/capacity, the historical depth high-water,
// the current busy window for single-goroutine lanes (owner/system/reply/
// custom), and recent per-call timing summaries. The pure lane reports no
// queue (depth/capacity 0, busy false) because stateless handlers never
// queue.
type LaneStats struct {
	Name      string          `json:"name"`
	Depth     int             `json:"depth"`
	Capacity  int             `json:"capacity"`
	HighWater int             `json:"highWater"`
	Busy      bool            `json:"busy"`
	BusyForNs uint64          `json:"busyForNs"`
	Timing    LaneTimingStats `json:"timing"`
}

// LaneTimingStats summarizes a lane's recent handler executions: nearest-
// rank p50/p99 and max over queue-wait and exec durations, plus the most
// recent ring entries (bounded, call IDs truncated) for per-(lane, callID)
// attribution.
type LaneTimingStats struct {
	Count     int               `json:"count"`
	WaitP50Ns uint64            `json:"waitP50Ns"`
	WaitP99Ns uint64            `json:"waitP99Ns"`
	WaitMaxNs uint64            `json:"waitMaxNs"`
	ExecP50Ns uint64            `json:"execP50Ns"`
	ExecP99Ns uint64            `json:"execP99Ns"`
	ExecMaxNs uint64            `json:"execMaxNs"`
	Recent    []LaneTimingEntry `json:"recent,omitempty"`
}

// LaneTimingEntry is one recent (callID, waitNs, execNs) observation.
type LaneTimingEntry struct {
	CallID string `json:"callID"`
	WaitNs uint64 `json:"waitNs"`
	ExecNs uint64 `json:"execNs"`
}

// CellStats is the public transport shape returned by the gospore.cell.stats
// root callable. All fields are point-in-time, non-blocking snapshots read via
// Cell.Stats().
type CellStats struct {
	ActorID        string     `json:"actorId"`
	ActorType      string     `json:"actorType"`
	State          string     `json:"state"`          // created/initialized/started/idle/stopped
	OwnerQueue     QueueStats `json:"ownerQueue"`     // stateful (owner) lane
	SystemQueue    QueueStats `json:"systemQueue"`    // system-message lane
	ReplyQueue     QueueStats `json:"replyQueue"`     // cross-actor reply lane
	PendingInvokes int        `json:"pendingInvokes"` // outbound calls awaiting response
	// Invoke carries this actor's pending-table diagnostics: in-flight
	// breakdowns by call mode (tell/unary/stream) and callable ID plus
	// lifetime failure counters. Each cell owns its table (the root's
	// doubles as the home for App/gateway-originated calls), so the numbers
	// reflect the actor's own registrations.
	Invoke InvokeStats `json:"invoke"`
	// Lanes carries per-lane observability (additive): queue depth,
	// capacity, depth high-water, busy window, and recent per-call timing
	// summaries for the owner/system/reply/custom/pure lanes. Consumers
	// that do not understand this field are unaffected — the JSON is
	// additive.
	Lanes []LaneStats `json:"lanes"`
}

// InvokeStats is the transport shape of invoke.InvokeStats.
type InvokeStats struct {
	InFlight        int                     `json:"inFlight"`
	InFlightByMode  map[string]int          `json:"inFlightByMode"`
	InFlightByCall  map[string]int          `json:"inFlightByCall"`
	Registered      uint64                  `json:"registered"`
	Completed       uint64                  `json:"completed"`
	Errored         uint64                  `json:"errored"`
	SendFailed      uint64                  `json:"sendFailed"`
	ClosedEarly     uint64                  `json:"closedEarly"`
	EvictedStalled  uint64                  `json:"evictedStalled"`
	EvictedCapacity uint64                  `json:"evictedCapacity"`
	Flushed         uint64                  `json:"flushed"`
	DroppedFrames   uint64                  `json:"droppedFrames"`
	RecentEvictions []invoke.EvictionRecord `json:"recentEvictions"`
}

// registerCellStatsCallable registers the gospore.cell.stats root callable,
// a stateless, public introspection endpoint returning a runtime snapshot of
// a target actor's ingress queues and pending outbound invocations.
func (a *appImpl) registerCellStatsCallable() error {
	if err := a.handlers.Register(cellStatsCallID, a.handleCellStats, actor.Public()); err != nil {
		return fmt.Errorf("register %s: %w", cellStatsCallID, err)
	}
	return nil
}

// handleCellStats resolves the target actor and returns its non-blocking
// runtime statistics. Reads are O(1) and safe from any goroutine.
func (a *appImpl) handleCellStats(_ actor.PureContext, req cellStatsReq) (any, error) {
	c, aid, err := a.resolveProjectionCell(req.ActorPath)
	if err != nil {
		return nil, err
	}
	rs := c.Stats()
	return CellStats{
		ActorID:   aid.String(),
		ActorType: cellActorType(c),
		State:     rs.State,
		OwnerQueue: QueueStats{
			Depth:    rs.OwnerQueueLen,
			Capacity: rs.OwnerQueueCap,
		},
		SystemQueue: QueueStats{
			Depth:    rs.SystemQueueLen,
			Capacity: rs.SystemQueueCap,
		},
		ReplyQueue: QueueStats{
			Depth:    rs.ReplyQueueLen,
			Capacity: rs.ReplyQueueCap,
		},
		PendingInvokes: rs.PendingInvokes,
		Lanes:          laneStatsTransport(rs.Lanes),
		Invoke: InvokeStats{
			InFlight:        rs.InvokeStats.InFlight,
			InFlightByMode:  rs.InvokeStats.InFlightByMode,
			InFlightByCall:  rs.InvokeStats.InFlightByCall,
			Registered:      rs.InvokeStats.Registered,
			Completed:       rs.InvokeStats.Completed,
			Errored:         rs.InvokeStats.Errored,
			SendFailed:      rs.InvokeStats.SendFailed,
			ClosedEarly:     rs.InvokeStats.ClosedEarly,
			EvictedStalled:  rs.InvokeStats.EvictedStalled,
			EvictedCapacity: rs.InvokeStats.EvictedCapacity,
			Flushed:         rs.InvokeStats.Flushed,
			DroppedFrames:   rs.InvokeStats.DroppedFrames,
			RecentEvictions: rs.InvokeStats.RecentEvictions,
		},
	}, nil
}

// laneStatsTransport converts the cell package's per-lane snapshot into the
// public transport shape.
func laneStatsTransport(lanes []cell.LaneRuntimeStats) []LaneStats {
	if len(lanes) == 0 {
		return nil
	}
	out := make([]LaneStats, 0, len(lanes))
	for _, l := range lanes {
		ls := LaneStats{
			Name:      l.Name,
			Depth:     l.Depth,
			Capacity:  l.Capacity,
			HighWater: l.HighWater,
			Busy:      l.Busy,
			BusyForNs: l.BusyForNs,
			Timing: LaneTimingStats{
				Count:     l.Timing.Count,
				WaitP50Ns: l.Timing.WaitP50Ns,
				WaitP99Ns: l.Timing.WaitP99Ns,
				WaitMaxNs: l.Timing.WaitMaxNs,
				ExecP50Ns: l.Timing.ExecP50Ns,
				ExecP99Ns: l.Timing.ExecP99Ns,
				ExecMaxNs: l.Timing.ExecMaxNs,
			},
		}
		if len(l.Recent) > 0 {
			ls.Timing.Recent = make([]LaneTimingEntry, 0, len(l.Recent))
			for _, r := range l.Recent {
				ls.Timing.Recent = append(ls.Timing.Recent, LaneTimingEntry{
					CallID: r.CallID,
					WaitNs: r.WaitNs,
					ExecNs: r.ExecNs,
				})
			}
		}
		out = append(out, ls)
	}
	return out
}

// cellActorType returns the actor's type string, guarding against a nil actor.
func cellActorType(c *cell.Cell) string {
	if a := c.Actor(); a != nil {
		return a.Type()
	}
	return ""
}
