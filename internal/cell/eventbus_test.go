package cell

import (
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
)

// TestEventBus_FanoutToManySubscribers verifies that a single Publish reaches
// every active subscriber on the matching (ns, kind) bucket.
func TestEventBus_FanoutToManySubscribers(t *testing.T) {
	bus := NewEventBus()

	const fanout = 4
	subs := make([]Subscription, fanout)
	cancels := make([]func(), fanout)
	for i := range subs {
		subs[i], cancels[i] = bus.Subscribe("ns", "k", id.Identity{})
	}
	defer func() {
		for _, c := range cancels {
			c()
		}
	}()

	bus.Publish("ns", "k", nil, "hello")

	deadline := time.After(time.Second)
	for i, s := range subs {
		select {
		case env := <-s.Ch:
			if  env.ActorId != "ns" || env.Kind != "k" || env.Payload != "hello" {
				t.Fatalf("sub %d: bad envelope %+v", i, env)
			}
		case <-deadline:
			t.Fatalf("sub %d: timeout waiting for envelope", i)
		}
	}
}

// TestEventBus_NoSubscribersDropsSilently verifies Publish with no
// subscribers returns immediately without panicking or blocking.
func TestEventBus_NoSubscribersDropsSilently(t *testing.T) {
	bus := NewEventBus()
	done := make(chan struct{})
	go func() {
		bus.Publish("ns", "k", nil, "x")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Publish blocked despite no subscribers")
	}
}

// TestEventBus_SubscribeByKind_ReceivesFromAnyEmitter verifies that a
// kind-level subscription receives events of that kind regardless of the
// emitting actor's identity or exposed services, and does not receive
// events of other kinds.
func TestEventBus_SubscribeByKind_ReceivesFromAnyEmitter(t *testing.T) {
	bus := NewEventBus()
	sub, cancel := bus.SubscribeByKind("aistats.record", id.Identity{})
	defer cancel()

	// Emit from two unrelated "actors", one with services, one without.
	bus.Publish("actor-AAA", "aistats.record", nil, 1)
	bus.Publish("actor-BBB", "aistats.record", []string{"aiaggregator"}, 2)
	// Different kind — must not be delivered.
	bus.Publish("actor-AAA", "other.kind", nil, 3)

	got := 0
	deadline := time.After(time.Second)
	for got < 2 {
		select {
		case env := <-sub.Ch:
			got++
			if env.Kind != "aistats.record" {
				t.Fatalf("unexpected kind delivered: %q", env.Kind)
			}
			if n, ok := env.Payload.(int); !ok || (n != 1 && n != 2) {
				t.Fatalf("unexpected payload: %v", env.Payload)
			}
		case <-deadline:
			t.Fatalf("timeout: received only %d of 2 envelopes", got)
		}
	}
	select {
	case env := <-sub.Ch:
		t.Fatalf("unexpected extra envelope: %+v", env)
	default:
	}
}

// TestEventBus_SubscribeByKind_Cancel verifies that cancelling a kind
// subscription stops delivery and cleans up the bucket.
func TestEventBus_SubscribeByKind_Cancel(t *testing.T) {
	bus := NewEventBus()
	sub, cancel := bus.SubscribeByKind("k", id.Identity{})
	if n := bus.SubscriberCountByKind("k"); n != 1 {
		t.Fatalf("SubscriberCountByKind = %d, want 1", n)
	}
	cancel()
	if n := bus.SubscriberCountByKind("k"); n != 0 {
		t.Fatalf("SubscriberCountByKind after cancel = %d, want 0", n)
	}
	select {
	case <-sub.Done:
	default:
		t.Fatal("Done not closed after cancel")
	}
	bus.Publish("ns", "k", nil, "x")
	select {
	case env := <-sub.Ch:
		t.Fatalf("envelope delivered after cancel: %+v", env)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestEventBus_NamespaceAndKindIsolation verifies that a Publish on
// (nsA, k) does not leak to (nsB, k) or (nsA, k2).
func TestEventBus_NamespaceAndKindIsolation(t *testing.T) {
	bus := NewEventBus()

	other1, c1 := bus.Subscribe("nsB", "k", id.Identity{})
	defer c1()
	other2, c2 := bus.Subscribe("nsA", "k2", id.Identity{})
	defer c2()
	target, c3 := bus.Subscribe("nsA", "k", id.Identity{})
	defer c3()

	bus.Publish("nsA", "k", nil, 42)

	select {
	case env := <-target.Ch:
		if env.Payload != 42 {
			t.Fatalf("target got %v, want 42", env.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("target subscriber did not receive event")
	}

	select {
	case env := <-other1.Ch:
		t.Fatalf("nsB subscriber leaked: %+v", env)
	case env := <-other2.Ch:
		t.Fatalf("k2 subscriber leaked: %+v", env)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestEventBus_CancelStopsDelivery verifies that after cancel, Done is
// closed and subsequent Publish does not deliver to the cancelled sub.
func TestEventBus_CancelStopsDelivery(t *testing.T) {
	bus := NewEventBus()
	sub, cancel := bus.Subscribe("ns", "k", id.Identity{})

	cancel()
	select {
	case <-sub.Done:
	case <-time.After(time.Second):
		t.Fatal("Done not closed after cancel")
	}

	cancel() // idempotent — must not panic.

	bus.Publish("ns", "k", nil, "after-cancel")
	select {
	case env := <-sub.Ch:
		t.Fatalf("received event after cancel: %+v", env)
	case <-time.After(50 * time.Millisecond):
	}

	if got := bus.SubscriberCount("ns", "k"); got != 0 {
		t.Fatalf("SubscriberCount after cancel = %d, want 0", got)
	}
}

// TestEventBus_DropOldestOnFullBuffer verifies that when a subscription's
// buffer is full, Publish drops the oldest queued event and the drops
// counter increments.
func TestEventBus_DropOldestOnFullBuffer(t *testing.T) {
	bus := NewEventBus()
	sub, cancel := bus.Subscribe("ns", "k", id.Identity{})
	defer cancel()

	for i := 0; i < subscriptionBufferCap; i++ {
		bus.Publish("ns", "k", nil, i)
	}

	if got := sub.Drops(); got != 0 {
		t.Fatalf("Drops after filling buffer = %d, want 0", got)
	}

	bus.Publish("ns", "k", nil, "overflow-1")

	if got := sub.Drops(); got == 0 {
		t.Fatalf("Drops after overflow = 0, want >= 1")
	}

	timeout := time.After(time.Second)
	drained := 0
	for {
		select {
		case <-sub.Ch:
			drained++
			if drained >= subscriptionBufferCap {
				return
			}
		case <-timeout:
			t.Fatalf("could only drain %d events, expected at least %d",
				drained, subscriptionBufferCap)
		}
	}
}

// TestEventBus_LaggingHookFires verifies that SetLaggingHook installs a
// callback that runs when Publish drops events for a slow subscriber.
// The hook receives the bus key, the subscription's identity, and the
// cumulative DropsTotal at the time of the drop.
func TestEventBus_LaggingHookFires(t *testing.T) {
	bus := NewEventBus()

	var (
		mu       sync.Mutex
		captured []LaggingEvent
	)
	bus.SetLaggingHook(func(ev LaggingEvent) {
		mu.Lock()
		defer mu.Unlock()
		captured = append(captured, ev)
	})

	ident := id.Identity{Kind: id.IdentityToken, Role: id.RoleAdmin, Subject: "alice"}
	_, cancel := bus.Subscribe("ns", "k", ident)
	defer cancel()

	for i := 0; i < subscriptionBufferCap; i++ {
		bus.Publish("ns", "k", nil, i)
	}

	mu.Lock()
	if len(captured) != 0 {
		mu.Unlock()
		t.Fatalf("hook fired before overflow: %+v", captured)
	}
	mu.Unlock()

	bus.Publish("ns", "k", nil, "overflow-1")

	mu.Lock()
	defer mu.Unlock()
	if len(captured) == 0 {
		t.Fatal("hook did not fire on overflow")
	}
	got := captured[0]
	if got.ActorId != "ns" || got.Kind != "k" {
		t.Fatalf("hook key = (%q,%q), want (ns,k)", got.ActorId, got.Kind)
	}
	if got.Identity != ident {
		t.Fatalf("hook identity = %+v, want %+v", got.Identity, ident)
	}
	if got.DropsTotal < 1 {
		t.Fatalf("hook DropsTotal = %d, want >= 1", got.DropsTotal)
	}
}

// TestEventBus_LaggingHookCleared verifies that SetLaggingHook(nil)
// removes a previously installed hook.
func TestEventBus_LaggingHookCleared(t *testing.T) {
	bus := NewEventBus()

	var fired atomic.Int32
	bus.SetLaggingHook(func(LaggingEvent) { fired.Add(1) })
	bus.SetLaggingHook(nil)

	_, cancel := bus.Subscribe("ns", "k", id.Identity{})
	defer cancel()

	for i := 0; i < subscriptionBufferCap+5; i++ {
		bus.Publish("ns", "k", nil, i)
	}

	if got := fired.Load(); got != 0 {
		t.Fatalf("hook fired %d times after clear, want 0", got)
	}
}

// TestEventBus_ReentrantPublishDoesNotDeadlock verifies that a subscriber
// goroutine which Publishes while reading does not deadlock the bus.
// Tests the snapshot-then-deliver design: Publish releases its RLock
// before pushing into channels.
func TestEventBus_ReentrantPublishDoesNotDeadlock(t *testing.T) {
	bus := NewEventBus()
	sub, cancel := bus.Subscribe("ns", "k1", id.Identity{})
	defer cancel()

	out, c2 := bus.Subscribe("ns", "k2", id.Identity{})
	defer c2()

	done := make(chan struct{})
	go func() {
		select {
		case <-sub.Ch:
			bus.Publish("ns", "k2", nil, "downstream")
		case <-time.After(time.Second):
			t.Errorf("first subscriber did not receive trigger")
		}
		close(done)
	}()

	bus.Publish("ns", "k1", nil, "trigger")

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reentrant publish deadlocked")
	}

	select {
	case env := <-out.Ch:
		if env.Payload != "downstream" {
			t.Fatalf("out got %+v, want downstream", env)
		}
	case <-time.After(time.Second):
		t.Fatal("downstream event never arrived")
	}
}

// TestEventBus_SubscriberCount verifies the count tracker reflects
// subscribe and cancel operations.
func TestEventBus_SubscriberCount(t *testing.T) {
	bus := NewEventBus()

	if got := bus.SubscriberCount("ns", "k"); got != 0 {
		t.Fatalf("empty bus count = %d, want 0", got)
	}

	_, c1 := bus.Subscribe("ns", "k", id.Identity{})
	_, c2 := bus.Subscribe("ns", "k", id.Identity{})
	_, c3 := bus.Subscribe("ns", "other", id.Identity{})

	if got := bus.SubscriberCount("ns", "k"); got != 2 {
		t.Fatalf("after 2 subs on (ns,k), count = %d, want 2", got)
	}
	if got := bus.SubscriberCount("ns", "other"); got != 1 {
		t.Fatalf("(ns,other) count = %d, want 1", got)
	}

	c1()
	if got := bus.SubscriberCount("ns", "k"); got != 1 {
		t.Fatalf("after one cancel, (ns,k) count = %d, want 1", got)
	}
	c2()
	c3()
	if got := bus.SubscriberCount("ns", "k"); got != 0 {
		t.Fatalf("after all cancels, (ns,k) count = %d, want 0", got)
	}
}

// TestEventBus_ConcurrentSubscribeAndPublish exercises the lock
// discipline under contention. It does not assert specific delivery
// counts (drop-oldest makes those non-deterministic) — it only verifies
// the bus does not deadlock or panic under concurrent load.
func TestEventBus_ConcurrentSubscribeAndPublish(t *testing.T) {
	bus := NewEventBus()
	const workers = 8
	const iters = 200

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, cancel := bus.Subscribe("ns", "k", id.Identity{})
				cancel()
			}
		}()
	}

	publishersDone := make(chan struct{})
	var pubWG sync.WaitGroup
	for i := 0; i < workers; i++ {
		pubWG.Add(1)
		go func(id int) {
			defer pubWG.Done()
			for j := 0; j < iters; j++ {
				bus.Publish("ns", "k", nil, id*1000+j)
			}
		}(i)
	}
	go func() {
		pubWG.Wait()
		close(publishersDone)
	}()

	select {
	case <-publishersDone:
	case <-time.After(5 * time.Second):
		t.Fatal("publishers timed out — likely deadlock")
	}

	close(stop)
	wg.Wait()
}

// --- ctx.EmitEvent integration tests ---

// TestEmitEvent_UnknownKindFails verifies that EmitEvent on a kind
// that was never registered returns DiagEventKindUnknown.
func TestEmitEvent_UnknownKindFails(t *testing.T) {
	c := newCellWithBus(t)
	ctx := NewStartContextForTest(c)

	err := ctx.EmitEvent("never-registered", struct{ X int }{X: 1})
	if err == nil {
		t.Fatal("expected error for unregistered kind, got nil")
	}
	if !strings.Contains(err.Error(), actor.DiagEventKindUnknown) {
		t.Fatalf("error = %q, want containing %s", err, actor.DiagEventKindUnknown)
	}
}

// TestEmitEvent_PayloadTypeMismatchFails verifies that emitting a
// payload whose runtime type differs from the registered example
// returns DiagEventPayloadMismatch.
func TestEmitEvent_PayloadTypeMismatchFails(t *testing.T) {
	c := newCellWithBus(t)
	ctx := NewStartContextForTest(c)

	type turnEvent struct{ Token string }
	if err := ctx.RegisterEventKind("turn", turnEvent{}); err != nil {
		t.Fatalf("RegisterEventKind: %v", err)
	}

	err := ctx.EmitEvent("turn", "wrong-type")
	if err == nil {
		t.Fatal("expected error for type mismatch, got nil")
	}
	if !strings.Contains(err.Error(), actor.DiagEventPayloadMismatch) {
		t.Fatalf("error = %q, want containing %s", err, actor.DiagEventPayloadMismatch)
	}
}

// TestEmitEvent_NoBusFails verifies that EmitEvent without a wired
// EventBus returns DiagEventNoBus.
func TestEmitEvent_NoBusFails(t *testing.T) {
	c := newCellNoBus(t)
	ctx := NewStartContextForTest(c)

	type ev struct{ X int }
	if err := ctx.RegisterEventKind("e", ev{}); err != nil {
		t.Fatalf("RegisterEventKind: %v", err)
	}

	err := ctx.EmitEvent("e", ev{X: 1})
	if err == nil {
		t.Fatal("expected error for missing bus, got nil")
	}
	if !strings.Contains(err.Error(), actor.DiagEventNoBus) {
		t.Fatalf("error = %q, want containing %s", err, actor.DiagEventNoBus)
	}
}

// TestEmitEvent_PublishesToBus verifies the happy path: registered kind
// + matching payload type + wired bus → subscriber receives the event.
func TestEmitEvent_PublishesToBus(t *testing.T) {
	c := newCellWithBus(t)
	ctx := NewStartContextForTest(c)

	type turnEvent struct{ Token string }
	if err := ctx.RegisterLoop("custom.events", actor.ModeStateful); err != nil {
		t.Fatalf("RegisterLoop: %v", err)
	}
	if err := ctx.RegisterEventKind("turn", turnEvent{}, actor.WithLoop("custom.events")); err != nil {
		t.Fatalf("RegisterEventKind: %v", err)
	}

	entry, ok := c.EventMetaEntries()["turn"]
	if !ok {
		t.Fatal("EventMetaEntries missing turn")
	}
	if entry.Loop != "custom.events" {
		t.Fatalf("loop = %q, want %q", entry.Loop, "custom.events")
	}

	sub, cancel := c.eventBus.Subscribe("test-ns", "turn", id.Identity{})
	defer cancel()

	if err := ctx.EmitEvent("turn", turnEvent{Token: "hi"}); err != nil {
		t.Fatalf("EmitEvent: %v", err)
	}

	select {
	case env := <-sub.Ch:
		got, ok := env.Payload.(turnEvent)
		if !ok {
			t.Fatalf("payload type = %T, want turnEvent", env.Payload)
		}
		if got.Token != "hi" {
			t.Fatalf("token = %q, want hi", got.Token)
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber did not receive event")
	}
}

// TestEmitEvent_AfterStopFails verifies that EmitEvent on the
// stop-context returns DiagEventAfterStop.
func TestEmitEvent_AfterStopFails(t *testing.T) {
	c := newCellWithBus(t)
	type ev struct{ X int }
	c.eventMeta["e"] = eventMetaEntry{
		Type: reflect.TypeOf(ev{}),
	}

	stopCtx := newStopContext(c)
	err := stopCtx.EmitEvent("e", ev{X: 1})
	if err == nil {
		t.Fatal("expected error from stop context, got nil")
	}
	if !strings.Contains(err.Error(), actor.DiagEventAfterStop) {
		t.Fatalf("error = %q, want containing %s", err, actor.DiagEventAfterStop)
	}
	if got := c.EventMetaEntries()["e"].Loop; got != "owner" && got != "" {
		t.Fatalf("event loop mutated unexpectedly: %q", got)
	}
}

// TestRegisterEventKind_AfterStartFails verifies that RegisterEventKind
// is rejected on the call-context (i.e. inside a request handler) so
// the eventMeta table stays write-once during OnStart.
func TestRegisterEventKind_AfterStartFails(t *testing.T) {
	c := newCellWithBus(t)
	callCtx := NewCallContextForTest(c)

	type ev struct{ X int }
	err := callCtx.RegisterEventKind("e", ev{})
	if err == nil {
		t.Fatal("expected error from call context, got nil")
	}
	if !strings.Contains(err.Error(), actor.DiagEventRegisterAfterStart) {
		t.Fatalf("error = %q, want containing %s", err, actor.DiagEventRegisterAfterStart)
	}
	if _, exists := c.eventMeta["e"]; exists {
		t.Fatal("eventMeta should not have been written by call-context RegisterEventKind")
	}
}

func TestRegisterLoop_StartContextRecordsCustomLoop(t *testing.T) {
	c := newCellWithBus(t)
	ctx := NewStartContextForTest(c)

	if err := ctx.RegisterLoop("custom.exec", actor.ModeStateful); err != nil {
		t.Fatalf("RegisterLoop: %v", err)
	}
	if err := ctx.RegisterLoop("custom.exec", actor.ModeStateless); err != nil {
		t.Fatalf("RegisterLoop second call: %v", err)
	}

	c.loopsMu.Lock()
	_, ok := c.loops["custom.exec"]
	c.loopsMu.Unlock()
	if !ok {
		t.Fatal("custom loop was not recorded")
	}
}

func TestRegisterLoop_ReplyLoopRejected(t *testing.T) {
	c := newCellWithBus(t)
	ctx := NewStartContextForTest(c)

	err := ctx.RegisterLoop(actor.DefaultLoopReply, actor.ModeStateful)
	if err == nil {
		t.Fatal("expected reply loop registration to fail")
	}
}

func TestRegisterLoop_AfterStartFails(t *testing.T) {
	c := newCellWithBus(t)
	callCtx := NewCallContextForTest(c)

	err := callCtx.RegisterLoop("custom.exec", actor.ModeStateful)
	if err == nil {
		t.Fatal("expected error from call context, got nil")
	}
	if !strings.Contains(err.Error(), actor.DiagEventRegisterAfterStart) {
		t.Fatalf("error = %q, want containing %s", err, actor.DiagEventRegisterAfterStart)
	}
}

func TestRegisterTimer_StartContextRecordsMetadata(t *testing.T) {
	c := newCellWithBus(t)
	ctx := NewStartContextForTest(c)

	if err := ctx.RegisterLoop("custom.exec", actor.ModeStateless); err != nil {
		t.Fatalf("RegisterLoop: %v", err)
	}
	if err := ctx.RegisterTimer("test.tick", actor.WithMode(actor.ModeStateless), actor.WithLoop("custom.exec")); err != nil {
		t.Fatalf("RegisterTimer: %v", err)
	}

	c.timerMetaMu.RLock()
	entry, ok := c.timerMeta["test.tick"]
	c.timerMetaMu.RUnlock()
	if !ok {
		t.Fatal("timer metadata missing")
	}
	if entry.Mode != actor.ModeStateless {
		t.Fatalf("timer mode = %v, want %v", entry.Mode, actor.ModeStateless)
	}
	if entry.Loop != "custom.exec" {
		t.Fatalf("timer loop = %q, want %q", entry.Loop, "custom.exec")
	}
}

func TestRegisterTimer_CustomLoopRequiresRegistration(t *testing.T) {
	c := newCellWithBus(t)
	ctx := NewStartContextForTest(c)

	err := ctx.RegisterTimer("test.tick", actor.WithLoop("custom.exec"))
	if err == nil {
		t.Fatal("expected unregistered custom timer loop to fail")
	}
}

func TestRegisterTimer_AfterStartFails(t *testing.T) {
	c := newCellWithBus(t)
	callCtx := NewCallContextForTest(c)

	err := callCtx.RegisterTimer("test.tick", actor.WithLoop("custom.exec"))
	if err == nil {
		t.Fatal("expected error from call context, got nil")
	}
	if !strings.Contains(err.Error(), actor.DiagEventRegisterAfterStart) {
		t.Fatalf("error = %q, want containing %s", err, actor.DiagEventRegisterAfterStart)
	}
}

// TestEventBus_SubscriptionStats verifies the diagnostics snapshot: every
// flavour appears with its routing key, cancel removes the entry, and a
// consumer that never drains reports full Backlog plus accumulated Drops.
func TestEventBus_SubscriptionStats(t *testing.T) {
	bus := NewEventBus()
	ident := id.Identity{Role: "admin"}

	_, cancelSvc := bus.SubscribeByService("workspace", "agents_changed", ident)
	_, _ = bus.SubscribeByInstance("actorA", "turn_step", ident)
	_, _ = bus.SubscribeByKind("agent_state", ident)

	stats := bus.SubscriptionStats()
	if len(stats) != 3 {
		t.Fatalf("subscriptions = %d, want 3: %+v", len(stats), stats)
	}
	byFlavor := map[string]SubInfo{}
	for _, s := range stats {
		byFlavor[s.Flavor] = s
	}
	if s := byFlavor["service"]; s.Match != "workspace" || s.Kind != "agents_changed" || s.Role != "admin" {
		t.Fatalf("service entry = %+v", s)
	}
	if s := byFlavor["instance"]; s.Match != "actorA" || s.Kind != "turn_step" {
		t.Fatalf("instance entry = %+v", s)
	}
	if s := byFlavor["kind"]; s.Match != "" || s.Kind != "agent_state" {
		t.Fatalf("kind entry = %+v", s)
	}
	if byFlavor["service"].Created.IsZero() {
		t.Fatal("Created not populated")
	}

	// Overflow the instance subscription's buffer without draining it.
	overflow := 3
	for i := 0; i < subscriptionBufferCap+overflow; i++ {
		bus.Publish("actorA", "turn_step", nil, "x")
	}
	stats = bus.SubscriptionStats()
	var inst SubInfo
	for _, s := range stats {
		if s.Flavor == "instance" && s.Match == "actorA" {
			inst = s
		}
	}
	if inst.Backlog != subscriptionBufferCap {
		t.Fatalf("Backlog = %d, want %d", inst.Backlog, subscriptionBufferCap)
	}
	if inst.Drops != int64(overflow) {
		t.Fatalf("Drops = %d, want %d", inst.Drops, overflow)
	}

	cancelSvc()
	stats = bus.SubscriptionStats()
	if n := len(stats); n != 2 {
		t.Fatalf("subscriptions after cancel = %d, want 2", n)
	}
	for _, s := range stats {
		if s.Flavor == "service" {
			t.Fatalf("cancelled service subscription still present: %+v", s)
		}
	}
}

// --- helpers ---

// newCellWithBus builds a minimal Cell wired with an EventBus, suitable
// for ctx.EmitEvent integration tests.
func newCellWithBus(t *testing.T) *Cell {
	t.Helper()
	return New(Config{
		Namespace: "test-ns",
		EventBus:  NewEventBus(),
	})
}

// newCellNoBus is like newCellWithBus but leaves EventBus nil so we
// can test the DiagEventNoBus path.
func newCellNoBus(t *testing.T) *Cell {
	t.Helper()
	return New(Config{Namespace: "test-ns"})
}
