package app

import (
	"fmt"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/internal/cell"
	"github.com/qomos-w/spore/identity"
)

const (
	eventSubscribeServiceCallID  = "gospore.events.subscribe_service"
	eventSubscribeInstanceCallID = "gospore.events.subscribe_instance"
	eventStatsCallID             = "gospore.events.stats"
)

type eventSubscribeServiceReq struct {
	ServiceName string `json:"serviceName"`
	Kind        string `json:"kind"`
}

type eventSubscribeInstanceReq struct {
	ActorId string `json:"actorId"`
	Kind    string `json:"kind"`
}

type eventManifestEntry struct {
	ActorPath  string
	Visibility actor.Visibility
	Kind       string
}

func (a *appImpl) registerEventSubscribeCallable() error {
	if err := a.handlers.Register(eventSubscribeServiceCallID, a.handleEventSubscribeService, actor.Public(), actor.Streaming[any]()); err != nil {
		return fmt.Errorf("register %s: %w", eventSubscribeServiceCallID, err)
	}
	if err := a.handlers.Register(eventSubscribeInstanceCallID, a.handleEventSubscribeInstance, actor.Public(), actor.Streaming[any]()); err != nil {
		return fmt.Errorf("register %s: %w", eventSubscribeInstanceCallID, err)
	}
	if err := a.handlers.Register(eventStatsCallID, a.handleEventStats, actor.Public()); err != nil {
		return fmt.Errorf("register %s: %w", eventStatsCallID, err)
	}
	return nil
}

func (a *appImpl) handleEventSubscribeService(ctx actor.PureContext, req eventSubscribeServiceReq, emit actor.Emitter) error {
	ctx.Logger().Info("eventbus: subscribe_service ENTER", "serviceName", req.ServiceName, "kind", req.Kind)
	if req.ServiceName == "" {
		return fmt.Errorf("gospore.events.subscribe_service: serviceName must not be empty")
	}
	if req.Kind == "" {
		return fmt.Errorf("gospore.events.subscribe_service: kind must not be empty")
	}

	meta, ok := a.lookupEventByService(req.ServiceName, req.Kind)
	if !ok {
		ctx.Logger().Warn("eventbus: subscribe_service event not found", "serviceName", req.ServiceName, "kind", req.Kind)
		return fmt.Errorf("gospore.events.subscribe_service: event %s.%s not found", req.ServiceName, req.Kind)
	}
	ctx.Logger().Info("eventbus: subscribe_service",
		"serviceName", req.ServiceName,
		"kind", meta.Kind,
		"actorPath", meta.ActorPath,
	)
	if !eventVisibilityAllowed(meta.Visibility, ctx.Identity()) {
		return fmt.Errorf("%s: event %s.%s visibility=%s", actor.DiagPolicyDenied, req.ServiceName, req.Kind, meta.Visibility.String())
	}
	if a.eventBus == nil {
		return fmt.Errorf("%s: event bus unavailable", actor.DiagEventNoBus)
	}

	sub, cancel := a.eventBus.SubscribeByService(req.ServiceName, req.Kind, ctx.Identity())
	defer func() {
		cancel()
		ctx.Logger().Info("eventbus: subscribe_service EXIT", "serviceName", req.ServiceName, "kind", req.Kind)
	}()

	return drainSubscription(ctx, sub, emit, req.ServiceName, req.Kind)
}

func (a *appImpl) handleEventSubscribeInstance(ctx actor.Context, req eventSubscribeInstanceReq, emit actor.Emitter) error {
	if req.ActorId == "" {
		return fmt.Errorf("gospore.events.subscribe_instance: actorId must not be empty")
	}
	if req.Kind == "" {
		return fmt.Errorf("gospore.events.subscribe_instance: kind must not be empty")
	}

	meta, ok := a.lookupEventByActorID(req.ActorId, req.Kind)
	if !ok {
		ctx.Logger().Warn("eventbus: subscribe_instance event not found", "actorId", req.ActorId, "kind", req.Kind)
		return fmt.Errorf("gospore.events.subscribe_instance: event %s.%s not found (actorId=%s)", req.ActorId, req.Kind, req.ActorId)
	}
	ctx.Logger().Info("eventbus: subscribe_instance",
		"actorId", req.ActorId,
		"kind", meta.Kind,
		"actorPath", meta.ActorPath,
		"actorId", req.ActorId,
	)
	if !eventVisibilityAllowed(meta.Visibility, ctx.Identity()) {
		return fmt.Errorf("%s: event %s.%s visibility=%s", actor.DiagPolicyDenied, req.ActorId, req.Kind, meta.Visibility.String())
	}
	if a.eventBus == nil {
		return fmt.Errorf("%s: event bus unavailable", actor.DiagEventNoBus)
	}

	sub, cancel := a.eventBus.SubscribeByInstance(req.ActorId, req.Kind, ctx.Identity())
	defer func() {
		cancel()
		ctx.Logger().Info("eventbus: subscribe_instance EXIT", "actorId", req.ActorId, "kind", req.Kind)
	}()

	return drainSubscription(ctx, sub, emit, req.ActorId, req.Kind)
}

type eventStatsReq struct{}

// eventStatsSub is the transport shape of one live subscription in the
// gospore.events.stats response.
type eventStatsSub struct {
	SubID   uint64 `json:"subId"`
	Flavor  string `json:"flavor"`
	Match   string `json:"match"`
	Kind    string `json:"kind"`
	Subject string `json:"subject"`
	Role    string `json:"role"`
	Backlog int    `json:"backlog"`
	Drops   int64  `json:"drops"`
	Created string `json:"created"`
}

type eventStatsResp struct {
	Subscriptions []eventStatsSub `json:"subscriptions"`
	// Rings lists retention rings that have evicted history
	// (evicted > 0), most-loss first. Silent ring overflow previously
	// surfaced only as subscriber gap markers; this makes the loss
	// itself queryable.
	Rings []eventStatsRing `json:"rings"`
	// StoreSubs lists the events store's per-subscription drop counters
	// (store-level Subscribe fan-out, distinct from the event bus above):
	// records lost to a full subscriber buffer. A slow consumer shows up
	// here even when its ring has never overflowed.
	StoreSubs []eventStatsStoreSub `json:"storeSubs"`
}

type eventStatsStoreSub struct {
	ActorID   string   `json:"actorId"`
	Kinds     []string `json:"kinds,omitempty"`
	Dropped   uint64   `json:"dropped"`
	BufferCap int      `json:"bufferCap"`
}

type eventStatsRing struct {
	ActorID  string `json:"actorId"`
	Len      int    `json:"len"`
	Capacity int    `json:"capacity"`
	Evicted  uint64 `json:"evicted"`
}

// handleEventStats serves gospore.events.stats: a point-in-time snapshot of
// every live event-bus subscription (flavor, routing key, subscriber identity,
// buffer backlog, cumulative drops, creation time). Intended for diagnosing
// subscription leaks and lagging consumers; contains no event payloads.
func (a *appImpl) handleEventStats(_ actor.PureContext, _ eventStatsReq) (eventStatsResp, error) {
	if a.eventBus == nil {
		return eventStatsResp{}, fmt.Errorf("%s: event bus unavailable", actor.DiagEventNoBus)
	}
	subs := a.eventBus.SubscriptionStats()
	out := make([]eventStatsSub, 0, len(subs))
	for _, s := range subs {
		out = append(out, eventStatsSub{
			SubID:   s.SubID,
			Flavor:  s.Flavor,
			Match:   s.Match,
			Kind:    s.Kind,
			Subject: s.Subject,
			Role:    s.Role,
			Backlog: s.Backlog,
			Drops:   s.Drops,
			Created: s.Created.UTC().Format(time.RFC3339Nano),
		})
	}
	rings := make([]eventStatsRing, 0)
	for _, rs := range a.eventsStore.RingStats() {
		if rs.Evicted == 0 {
			continue
		}
		rings = append(rings, eventStatsRing{
			ActorID:  rs.ActorID.String(),
			Len:      rs.Len,
			Capacity: rs.Capacity,
			Evicted:  rs.Evicted,
		})
	}
	storeSubs := make([]eventStatsStoreSub, 0)
	for _, ss := range a.eventsStore.SubscriberStats() {
		if ss.Dropped == 0 {
			continue
		}
		kinds := make([]string, 0, len(ss.Kinds))
		for _, k := range ss.Kinds {
			kinds = append(kinds, k.String())
		}
		storeSubs = append(storeSubs, eventStatsStoreSub{
			ActorID:   ss.ActorID.String(),
			Kinds:     kinds,
			Dropped:   ss.Dropped,
			BufferCap: ss.BufferCap,
		})
	}
	return eventStatsResp{Subscriptions: out, Rings: rings, StoreSubs: storeSubs}, nil
}

// drainSubscription pumps events from sub to emit until ctx or emit signals done.
func drainSubscription(ctx actor.PureContext, sub cell.Subscription, emit actor.Emitter, routingKey, kind string) error {
	ctx.Logger().Info("eventbus: drainSubscription START", "routingKey", routingKey, "kind", kind)
	defer ctx.Logger().Info("eventbus: drainSubscription EXIT", "routingKey", routingKey, "kind", kind)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-emit.Done():
			return nil
		case <-sub.Done:
			return nil
		case env, ok := <-sub.Ch:
			if !ok {
				return nil
			}
			ctx.Logger().Debug("eventbus: drainSubscription sending chunk", "routingKey", routingKey, "kind", kind, "actorId", env.ActorId)
			if err := emit.Send(env.Payload); err != nil {
				return err
			}
		}
	}
}

// lookupEventByService finds the cell that registered the given service name
// and checks that it has the requested event kind registered. Returns the
// event metadata with the cell's actor ID as the routing key.
func (a *appImpl) lookupEventByService(serviceName, kind string) (eventManifestEntry, bool) {
	a.cellMu.RLock()
	cells := make([]*cell.Cell, 0, len(a.cells))
	for _, c := range a.cells {
		if c != nil {
			cells = append(cells, c)
		}
	}
	a.cellMu.RUnlock()

	for _, c := range cells {
		for _, svc := range c.Services() {
			if svc != serviceName {
				continue
			}
			entries := c.EventMetaEntries()
			entry, ok := entries[kind]
			if !ok {
				return eventManifestEntry{}, false
			}
			actorPath := c.Self().ID().String()
			return eventManifestEntry{
				ActorPath:  actorPath,
				Visibility: entry.Visibility,
				
				Kind:       kind,
			}, true
		}
	}
	return eventManifestEntry{}, false
}

// lookupEventByActorID finds the cell by its canonical actor ID and checks
// that it has the requested event kind registered. Returns the event metadata
// with the actor ID as the routing key.
func (a *appImpl) lookupEventByActorID(actorIDHex, kind string) (eventManifestEntry, bool) {
	a.cellMu.RLock()
	cells := make([]*cell.Cell, 0, len(a.cells))
	for _, c := range a.cells {
		if c != nil {
			cells = append(cells, c)
		}
	}
	a.cellMu.RUnlock()

	cid, err := identity.ParseCanonicalID(actorIDHex)
	if err != nil {
		return eventManifestEntry{}, false
	}
	aid := id.From(cid)

	for _, c := range cells {
		if c.Self().ID() != aid {
			continue
		}
		entries := c.EventMetaEntries()
		entry, ok := entries[kind]
		if !ok {
			return eventManifestEntry{}, false
		}
			actorPath := c.Self().ID().String()
		return eventManifestEntry{
			ActorPath:  actorPath,
			Visibility: entry.Visibility,
			
			Kind:       kind,
		}, true
	}
	return eventManifestEntry{}, false
}

func eventVisibilityAllowed(visibility actor.Visibility, ident id.Identity) bool {
	switch visibility {
	case actor.VisibilityPublic:
		return true
	case actor.VisibilityAdmin, actor.VisibilityDiagnostic:
		return ident.Role == id.RoleAdmin
	default:
		return false
	}
}
