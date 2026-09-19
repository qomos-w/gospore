package app

import (
	"fmt"
	"reflect"
	"unicode"
	"unicode/utf8"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/spore/binding"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/internal/cell"
	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/projection"
	"github.com/qomos-w/spore/identity"
)

const (
	projectionGetCallID   = "gospore.projection.get"
	projectionWatchCallID = "gospore.projection.watch"
)

type projectionGetReq struct {
	ActorPath string `json:"actorPath"`
	Component string `json:"component"`
	SchemaID  uint64 `json:"schemaId"`
}

type projectionWatchReq struct {
	ActorPath string `json:"actorPath"`
	Component string `json:"component"`
	SchemaID  uint64 `json:"schemaId"`
}

func (a *appImpl) registerProjectionCallables() error {
	if err := a.handlers.Register(projectionGetCallID, a.handleProjectionGet, actor.Public()); err != nil {
		return fmt.Errorf("register %s: %w", projectionGetCallID, err)
	}
	if err := a.handlers.Register(projectionWatchCallID, a.handleProjectionWatch, actor.Public(), actor.Streaming[any]()); err != nil {
		return fmt.Errorf("register %s: %w", projectionWatchCallID, err)
	}
	return nil
}

func (a *appImpl) handleProjectionGet(ctx actor.PureContext, req projectionGetReq) (any, error) {
	c, aid, err := a.resolveProjectionCell(req.ActorPath)
	if err != nil {
		return nil, err
	}
	if err := a.checkProjectionVisibility(c, req.Component, ctx.Identity()); err != nil {
		return nil, err
	}
	fieldKey := lowerFirst(req.Component)
	field, ok := a.projStore.GetField(aid, fieldKey)
	if !ok {
		// No snapshot has been materialised yet. Become a temporary subscriber
		// so the cell will compute on refresh, then read the first published update.
		sub, err := a.projStore.Watch(aid)
		if err != nil {
			return nil, fmt.Errorf("gospore.projection.get: %w", err)
		}
		defer sub.Close()
		if err := a.refreshProjection(aid); err != nil {
			return nil, err
		}
		u, err := sub.Recv()
		if err != nil {
			return nil, fmt.Errorf("gospore.projection.get: %w", err)
		}
		return a.extractProjectionComponent(c, aid, req.Component, fieldKey, u)
	}
	return componentValue(field, c, req.Component), nil
}

// handleProjectionWatch is a stateless streaming handler (PureContext): it
// runs on the invocation's fork goroutine, never on the root owner lane, so
// an active watch cannot starve stateful callables such as gospore.cell.stats
// (ARCHITECTURE.md §4.12 / OwnerLane constraint). The subscription is
// established here, then the drain pumps projection updates until any
// terminal signal fires. The triple exit (ctx.Done / emit.Done / sub.Done)
// closes the subscription, which immediately unblocks the blocking Recv with
// io.EOF — cancellation does not wait for the next projection update.
func (a *appImpl) handleProjectionWatch(ctx actor.PureContext, req projectionWatchReq, emit actor.Emitter) error {
	c, aid, err := a.resolveProjectionCell(req.ActorPath)
	if err != nil {
		return err
	}
	if err := a.checkProjectionVisibility(c, req.Component, ctx.Identity()); err != nil {
		return err
	}

	sub, err := a.projStore.Watch(aid)
	if err != nil {
		return fmt.Errorf("gospore.projection.watch: %w", err)
	}
	defer sub.Close()

	fieldKey := lowerFirst(req.Component)
	// Triple exit: any terminal signal closes the subscription so the blocking
	// Recv below returns io.EOF immediately, instead of lingering until the
	// next projection update (or forever). sub.Close is guarded by sync.Once
	// and is also invoked via the defer above, so calling it here is safe.
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
		case <-emit.Done():
		case <-sub.Done():
		}
		close(done)
		_ = sub.Close()
	}()

	for {
		select {
		case <-done:
			return nil
		default:
		}
		u, err := sub.Recv()
		if err != nil {
			return nil
		}
		value, err := a.extractProjectionComponent(c, aid, req.Component, fieldKey, u)
		if err != nil {
			return err
		}
		if err := emit.Send(value); err != nil {
			return err
		}
	}
}

func (a *appImpl) refreshProjection(aid id.ActorID) error {
	targetRef, ok := a.tree.LookupID(aid)
	if !ok {
		return fmt.Errorf("gospore.projection: actor %s not found in tree", aid.String())
	}
	if err := a.deliver(targetRef, mailbox.Envelope{Payload: mailbox.RefreshProjection{}}); err != nil {
		return fmt.Errorf("gospore.projection: refresh %s: %w", aid.String(), err)
	}
	return nil
}

func (a *appImpl) resolveProjectionCell(actorPath string) (*cell.Cell, id.ActorID, error) {
	if actorPath == "" {
		return nil, id.ActorID{}, fmt.Errorf("gospore.projection: actorPath must not be empty")
	}

	// Fast path: actorPath is a canonical ULID → resolve directly.
	if cid, err := identity.ParseCanonicalID(actorPath); err == nil {
		aid := id.From(cid)
		if c := a.getCell(aid); c != nil {
			return c, aid, nil
		}
		return nil, id.ActorID{}, fmt.Errorf("gospore.projection: actor %s not found", actorPath)
	}

	// Fallback: actorPath is a service name (e.g. "workspace", "appmanager")
	// → resolve via the service registry. This supports codegen that emits
	// stable logical identifiers instead of ephemeral runtime ULIDs.
	if ref, ok := a.svcReg.Lookup(actorPath); ok {
		aid := ref.ID()
		if c := a.getCell(aid); c != nil {
			return c, aid, nil
		}
	}

	// Last resort: treat actorPath as an actor type and scan cells.
	a.cellMu.RLock()
	for aid, c := range a.cells {
		if c != nil && c.Actor().Type() == actorPath {
			a.cellMu.RUnlock()
			return c, aid, nil
		}
	}
	a.cellMu.RUnlock()

	return nil, id.ActorID{}, fmt.Errorf("gospore.projection: actor %q not found (not a valid ULID or known service/type)", actorPath)
}

func (a *appImpl) checkProjectionVisibility(c *cell.Cell, component string, ident id.Identity) error {
	for _, slot := range c.ComponentSlots() {
		if slot.Name != component {
			continue
		}
		if projectionVisibilityAllowed(slot.Visibility, ident) {
			return nil
		}
		return fmt.Errorf("%s: projection %q visibility=%s", actor.DiagPolicyDenied, component, slot.Visibility.String())
	}
	return nil
}

func (a *appImpl) extractProjectionComponent(c *cell.Cell, aid id.ActorID, component, fieldKey string, u projection.Update) (any, error) {
	var field binding.ViewProjection
	var ok bool
	switch u.Kind {
	case projection.UpdateFullSnapshot, projection.UpdateGapTooLarge:
		field, ok = u.Snapshot.Fields[fieldKey]
		if !ok {
			return nil, fmt.Errorf("gospore.projection: component %q not found in snapshot", component)
		}
	case projection.UpdateDelta:
		if field, ok = u.Delta[fieldKey]; ok {
			break
		}
		field, ok = a.projStore.GetField(aid, fieldKey)
		if !ok {
			return nil, fmt.Errorf("gospore.projection: component %q not found", component)
		}
	default:
		return nil, fmt.Errorf("gospore.projection: unknown update kind %q", u.Kind)
	}
	return componentValue(field, c, component), nil
}

// componentValue unwraps a ViewProjection into the transport value expected by
// clients. Struct components are projected as a field-name map; everything else
// is returned as the raw scalar/slice/map value.
func componentValue(vp binding.ViewProjection, c *cell.Cell, component string) any {
	if !componentIsStruct(c, component) {
		if v, ok := vp.Fields["value"]; ok {
			return v
		}
	}
	return vp.Fields
}

func componentIsStruct(c *cell.Cell, component string) bool {
	for _, slot := range c.ComponentSlots() {
		if slot.Name != component {
			continue
		}
		t := slot.Type
		for t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		return t.Kind() == reflect.Struct
	}
	return false
}

func projectionVisibilityAllowed(visibility actor.Visibility, ident id.Identity) bool {
	switch visibility {
	case actor.VisibilityPublic:
		return true
	case actor.VisibilityAdmin, actor.VisibilityDiagnostic:
		return ident.Role == id.RoleAdmin
	default:
		return false
	}
}

func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	r, size := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError && size == 0 {
		return s
	}
	return string(unicode.ToLower(r)) + s[size:]
}
