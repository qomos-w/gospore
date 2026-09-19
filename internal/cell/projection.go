package cell

import (
	"fmt"
	"runtime/debug"

	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/projection"
	"github.com/qomos-w/spore/binding"
)

func (c *Cell) applyProjectionIfObserved() (version uint64, changed bool, err error) {
	// Snapshot dereferences live actor state and can panic if a concurrent
	// goroutine mutates the field tree (slice grow/shrink, nil map, etc).
	// projection.snapshotValue already recovers internally, but we keep this
	// outer guard so any future reflect-side bug stays contained: a projection
	// failure must never bubble up to invokeCall's defer → recoverAndDecide,
	// which would otherwise Restart the actor and (for workspace) pause every
	// running agent in OnStart. This mirrors the gospore convention that
	// business errors do not consult the supervisor.
	defer func() {
		if r := recover(); r != nil {
			if c.logger != nil {
				c.logger.Error("gospore/cell: projection panic recovered",
					"actorID", c.cellID(),
					"panic", r,
					"stack", string(debug.Stack()),
				)
			}
			err = fmt.Errorf("projection panic: %v", r)
			c.projectionMu.Lock()
			version = c.projectionVersion
			c.projectionMu.Unlock()
			changed = false
		}
	}()
	c.projectionMu.Lock()
	defer c.projectionMu.Unlock()
	if c == nil || c.projections == nil || c.self == nil || !c.projections.HasSubscribers(c.self.ID()) {
		return c.projectionVersion, false, nil
	}
	var next projection.FieldSnapshot
	slots := c.ComponentSlots()
	if len(slots) > 0 {
		next, err = projection.SnapshotOfSlots(c.actor, slots)
	} else {
		next, err = projection.SnapshotOf(c.actor)
	}
	if err != nil {
		return c.projectionVersion, false, err
	}
	delta := projection.Diff(c.projectionState, next)
	if len(delta) == 0 {
		return c.projectionVersion, false, nil
	}
	c.projectionState = next
	c.projectionVersion++
	c.projections.Publish(c.self.ID(), projection.Snapshot{
		ActorID: c.self.ID(),
		Version: c.projectionVersion,
		Fields:  materializeProjectionFields(next),
	})
	return c.projectionVersion, true, nil
}

func materializeProjectionFields(state projection.FieldSnapshot) map[string]binding.ViewProjection {
	if len(state) == 0 {
		return nil
	}
	out := make(map[string]binding.ViewProjection, len(state))
	for name, value := range state {
		fields, ok := value.(projection.FieldSnapshot)
		if !ok {
			fields = projection.FieldSnapshot{"value": value}
		}
		out[name] = binding.ViewProjection{Fields: map[string]any(fields)}
	}
	return out
}

func ApplyProjectionForTest(a any) (uint64, bool, error) {
	next, err := projection.SnapshotOf(a)
	if err != nil {
		return 0, false, err
	}
	if len(next) == 0 {
		return 0, false, nil
	}
	store := projection.NewStore(0)
	aid := id.ActorID{}
	store.Publish(aid, projection.Snapshot{ActorID: aid, Version: 1, Fields: materializeProjectionFields(next)})
	return 1, true, nil
}
