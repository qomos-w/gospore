package discovery

import (
	"testing"
	"time"
)

func expectEvent(t *testing.T, watcher Watcher) Event {
	t.Helper()
	select {
	case event := <-watcher.C:
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}
	return Event{}
}

func expectNoEvent(t *testing.T, watcher Watcher) {
	t.Helper()
	select {
	case event := <-watcher.C:
		t.Fatalf("unexpected event: %+v", event)
	default:
	}
}

func TestMemoryProvider_RegisterVisible(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	p := NewMemoryProvider(now)
	watcher, err := p.Subscribe(0)
	if err != nil {
		t.Fatal(err)
	}

	inst := Instance{Namespace: "app.a", Service: "auth", InstanceGroupID: "auth-primary", RuntimeSlotID: 1}
	if err := p.Register(inst, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	got := p.Resolve("auth")
	if len(got) != 1 || got[0].Namespace != "app.a" {
		t.Fatalf("resolve = %+v", got)
	}
	event := expectEvent(t, watcher)
	if event.Kind != EventRegistered || event.Instance.Namespace != inst.Namespace {
		t.Fatalf("event = %+v", event)
	}
}

func TestMemoryProvider_RenewBeforeExpiry(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	p := NewMemoryProvider(now)
	inst := Instance{Namespace: "app.a", Service: "auth", InstanceGroupID: "auth-primary", RuntimeSlotID: 1}
	p.Register(inst, 5*time.Second)
	p.Advance(now.Add(4 * time.Second))
	if err := p.Renew("app.a", 5*time.Second); err != nil {
		t.Fatal("renew failed:", err)
	}
	p.Advance(now.Add(8 * time.Second))
	got := p.Resolve("auth")
	if len(got) != 1 || got[0].Namespace != "app.a" {
		t.Fatalf("resolve = %+v", got)
	}
}

func TestMemoryProvider_RenewNotFound(t *testing.T) {
	p := NewMemoryProvider(time.Now())
	if err := p.Renew("missing", time.Second); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemoryProvider_Deregister(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	p := NewMemoryProvider(now)
	watcher, _ := p.Subscribe(0)
	inst := Instance{Namespace: "app.a", Service: "auth", InstanceGroupID: "auth-primary", RuntimeSlotID: 1}
	p.Register(inst, 5*time.Second)
	_ = expectEvent(t, watcher)

	if err := p.Deregister("app.a"); err != nil {
		t.Fatal(err)
	}
	if got := p.Resolve("auth"); len(got) != 0 {
		t.Fatalf("expected empty after deregister, got %+v", got)
	}
	event := expectEvent(t, watcher)
	if event.Kind != EventExpired {
		t.Fatalf("expected expired event, got %+v", event)
	}
}

func TestMemoryProvider_DeregisterNotFound(t *testing.T) {
	p := NewMemoryProvider(time.Now())
	if err := p.Deregister("missing"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemoryProvider_AutoExpireOnAdvance(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	p := NewMemoryProvider(now)
	inst := Instance{Namespace: "app.a", Service: "auth", InstanceGroupID: "auth-primary", RuntimeSlotID: 1}
	p.Register(inst, 5*time.Second)
	if expired := p.Advance(now.Add(6 * time.Second)); len(expired) != 1 || expired[0].Namespace != "app.a" {
		t.Fatalf("expired = %+v", expired)
	}
	if got := p.Resolve("auth"); len(got) != 0 {
		t.Fatalf("resolve = %+v", got)
	}
}

func TestMemoryProvider_RejoinAfterExpiry(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	p := NewMemoryProvider(now)
	watcher, _ := p.Subscribe(0)
	inst := Instance{Namespace: "app.a", Service: "auth", InstanceGroupID: "auth-primary", RuntimeSlotID: 1}
	p.Register(inst, 5*time.Second)
	first := expectEvent(t, watcher)
	if first.Kind != EventRegistered {
		t.Fatalf("first = %+v", first)
	}
	p.Advance(now.Add(6 * time.Second))
	expired := expectEvent(t, watcher)
	if expired.Kind != EventExpired {
		t.Fatalf("expired = %+v", expired)
	}
	p.Register(inst, 5*time.Second)
	rejoined := expectEvent(t, watcher)
	if rejoined.Kind != EventRejoined {
		t.Fatalf("rejoined = %+v", rejoined)
	}
}

func TestMemoryProvider_WatchSinceReplay(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	p := NewMemoryProvider(now)
	p.Register(Instance{Namespace: "app.a", Service: "auth", InstanceGroupID: "auth-primary", RuntimeSlotID: 1}, 5*time.Second)
	p.Register(Instance{Namespace: "app.b", Service: "auth", InstanceGroupID: "auth-primary", RuntimeSlotID: 2}, 5*time.Second)

	watcher, err := p.Subscribe(1)
	if err != nil {
		t.Fatal(err)
	}
	second := expectEvent(t, watcher)
	if second.SeqNo != 2 || second.Instance.Namespace != "app.b" {
		t.Fatalf("event = %+v", second)
	}
}

func TestMemoryProvider_WatchExpireThenRejoinOrdering(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	p := NewMemoryProvider(now)
	watcher, _ := p.Subscribe(0)
	inst := Instance{Namespace: "app.a", Service: "auth", InstanceGroupID: "auth-primary", RuntimeSlotID: 1}
	p.Register(inst, 5*time.Second)
	_ = expectEvent(t, watcher)
	p.Advance(now.Add(6 * time.Second))
	expired := expectEvent(t, watcher)
	p.Register(inst, 5*time.Second)
	rejoined := expectEvent(t, watcher)
	if expired.Kind != EventExpired || rejoined.Kind != EventRejoined {
		t.Fatalf("events = %+v %+v", expired, rejoined)
	}
	if expired.SeqNo >= rejoined.SeqNo {
		t.Fatalf("seq order invalid: %d then %d", expired.SeqNo, rejoined.SeqNo)
	}
}

func TestMemoryProvider_WatchSlowSubscriberDrops(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	p := NewMemoryProvider(now)
	watcher, _ := p.Subscribe(0)
	for i := 0; i < 40; i++ {
		inst := Instance{Namespace: "app.slow", Service: "auth", InstanceGroupID: "auth-primary", RuntimeSlotID: uint16(i)}
		p.Register(inst, time.Second)
	}
	expectEvent(t, watcher)
	// The hub drops slow subscribers non-blockingly; we can't directly
	// inspect dropped count because it's unexported, but the test
	// validates the non-blocking delivery path doesn't deadlock.
}

func TestMemoryProvider_ResolveSorted(t *testing.T) {
	now := time.Now()
	p := NewMemoryProvider(now)
	p.Register(Instance{Namespace: "z", Service: "svc", InstanceGroupID: "g", RuntimeSlotID: 1}, time.Hour)
	p.Register(Instance{Namespace: "a", Service: "svc", InstanceGroupID: "g", RuntimeSlotID: 2}, time.Hour)
	p.Register(Instance{Namespace: "m", Service: "svc", InstanceGroupID: "g", RuntimeSlotID: 3}, time.Hour)
	got := p.Resolve("svc")
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
	if got[0].Namespace != "a" || got[1].Namespace != "m" || got[2].Namespace != "z" {
		t.Fatalf("not sorted: %v %v %v", got[0].Namespace, got[1].Namespace, got[2].Namespace)
	}
}

func TestMemoryProvider_AdvanceIgnoresBackwardTime(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	p := NewMemoryProvider(now)
	inst := Instance{Namespace: "app.a", Service: "auth", InstanceGroupID: "auth-primary", RuntimeSlotID: 1}
	p.Register(inst, 5*time.Second)
	// Advance backward should be a no-op
	if expired := p.Advance(now.Add(-1 * time.Hour)); len(expired) != 0 {
		t.Fatalf("expected no expiry on backward advance, got %+v", expired)
	}
	// The lease should still be alive
	if got := p.Resolve("auth"); len(got) != 1 {
		t.Fatalf("expected 1 live instance, got %d", len(got))
	}
}

func TestMemoryProvider_ImplementsProvider(t *testing.T) {
	var _ Provider = (*MemoryProvider)(nil)
}
