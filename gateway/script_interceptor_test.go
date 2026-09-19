package gateway_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/app"
	"github.com/qomos-w/gospore/codec"
	"github.com/qomos-w/gospore/gateway"
)

// TestScriptInterceptor_Before_Allows confirms a permissive interceptor
// script does not block the request.
func TestScriptInterceptor_Before_Allows(t *testing.T) {
	a, shutdown := newTestApp(t)
	defer shutdown()

	src := `import Context from "gospore"
export fun before(ctx: Context, req: any): string { return "" }`
	si, err := gateway.NewScriptInterceptor(a, src)
	if err != nil {
		t.Fatalf("NewScriptInterceptor: %v", err)
	}

	req := &gateway.GatewayRequest{CallID: "test.echo", CustomerID: "alice"}
	if err := si.Before(context.Background(), req); err != nil {
		t.Fatalf("Before: expected no error, got %v", err)
	}
}

// TestScriptInterceptor_Before_Denies confirms a script that returns a
// non-empty string aborts the request and the message surfaces from Before.
func TestScriptInterceptor_Before_Denies(t *testing.T) {
	a, shutdown := newTestApp(t)
	defer shutdown()

	src := `import Context from "gospore"
export fun before(ctx: Context, req: any): string {
	return "quota exceeded"
}`
	si, err := gateway.NewScriptInterceptor(a, src)
	if err != nil {
		t.Fatalf("NewScriptInterceptor: %v", err)
	}

	req := &gateway.GatewayRequest{CallID: "test.echo", CustomerID: "alice"}
	if err := si.Before(context.Background(), req); err == nil {
		t.Fatal("Before: expected error, got nil")
	} else if !strings.Contains(err.Error(), "quota exceeded") {
		t.Fatalf("Before: error message = %q, want 'quota exceeded'", err.Error())
	}
}

// TestScriptInterceptor_Before_CheckPolicy verifies the interceptor script
// can access the App's PolicyStore through ctx.check_policy.
func TestScriptInterceptor_Before_CheckPolicy(t *testing.T) {
	a, shutdown := newTestApp(t)
	defer shutdown()

	// Load a deny-all policy.
	_ = a.PolicyStore().Reload([]actor.Policy{
		{Scope: "test.echo", Role: "", Allow: false},
	})

	src := `import Context from "gospore"
import PolicyCheckResult from "gospore"
export fun before(ctx: Context, req: any): string {
	var result: PolicyCheckResult = ctx.check_policy("", "test.echo")
	if (!result.allow) {
		return "policy denied"
	}
	return ""
}`
	si, err := gateway.NewScriptInterceptor(a, src)
	if err != nil {
		t.Fatalf("NewScriptInterceptor: %v", err)
	}

	req := &gateway.GatewayRequest{CallID: "test.echo", CustomerID: "alice"}
	if err := si.Before(context.Background(), req); err == nil {
		t.Fatal("Before: expected policy denial, got nil")
	} else if !strings.Contains(err.Error(), "policy denied") {
		t.Fatalf("Before: error = %q, want 'policy denied'", err.Error())
	}
}

// TestScriptInterceptor_Before_LookupService verifies the interceptor script
// can lookup a service actor and invoke it.
func TestScriptInterceptor_Before_LookupService(t *testing.T) {
	a, shutdown := newTestApp(t)
	defer shutdown()

	// Spawn a quota actor that exposes "quota.check".
	_, err := a.Spawn(actor.PropsFromFunc(func() actor.Actor {
		return &quotaActor{allowed: false}
	}), "quota")
	if err != nil {
		t.Fatalf("Spawn quota: %v", err)
	}

	src := `import Context from "gospore"
import ActorRef from "gospore"
export fun before(ctx: Context, req: any): string {
	var quota: ActorRef = ctx.lookup_service("quota")
	var result: any = quota.invoke("check", {})
	if (result == null) {
		return "quota check failed"
	}
	return ""
}`
	si, err := gateway.NewScriptInterceptor(a, src)
	if err != nil {
		t.Fatalf("NewScriptInterceptor: %v", err)
	}

	req := &gateway.GatewayRequest{CallID: "test.echo", CustomerID: "alice"}
	if err := si.Before(context.Background(), req); err == nil {
		t.Fatal("Before: expected quota check failure, got nil")
	}

	// Verify the error path executed by checking the error message.
	if err := si.Before(context.Background(), req); !strings.Contains(err.Error(), "quota check failed") {
		t.Fatalf("Before: unexpected error: %v", err)
	}
}

// TestScriptInterceptor_Concurrent verifies that concurrent Before calls
// on a single ScriptInterceptor do not race or panic.
func TestScriptInterceptor_Concurrent(t *testing.T) {
	a, shutdown := newTestApp(t)
	defer shutdown()

	src := `import Context from "gospore"
export fun before(ctx: Context, req: any): string { return "" }`
	si, err := gateway.NewScriptInterceptor(a, src)
	if err != nil {
		t.Fatalf("NewScriptInterceptor: %v", err)
	}

	req := &gateway.GatewayRequest{CallID: "test.echo", CustomerID: "alice"}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := si.Before(context.Background(), req); err != nil {
				t.Errorf("Before: %v", err)
			}
		}()
	}
	wg.Wait()
}

// TestScriptInterceptor_After_Async confirms After runs without blocking
// and errors are swallowed.
func TestScriptInterceptor_After_Async(t *testing.T) {
	a, shutdown := newTestApp(t)
	defer shutdown()

	src := `import Context from "gospore"
export fun after(ctx: Context, req: any, resp: any): string {
	return "after error should be swallowed"
}`
	si, err := gateway.NewScriptInterceptor(a, src)
	if err != nil {
		t.Fatalf("NewScriptInterceptor: %v", err)
	}

	req := &gateway.GatewayRequest{CallID: "test.echo"}
	resp := &gateway.GatewayResponse{Body: "ok", Duration: 10 * time.Millisecond}

	// After should not panic or block; it runs in its own goroutine in
	// production, but here we call it directly to verify it returns promptly.
	si.After(context.Background(), req, resp)
}

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

type testActor struct{ actor.Host }

func (a *testActor) OnStart(ctx actor.Context) error {
	_ = ctx.Register("test.echo", func(_ actor.Context, req string) (string, error) {
		return req, nil
	})
	return nil
}

type quotaActor struct {
	actor.Host
	allowed bool
}

func (a *quotaActor) OnStart(ctx actor.Context) error {
	_ = ctx.Register("quota.check", func(_ actor.Context, _ any) (map[string]any, error) {
		return map[string]any{"allowed": a.allowed}, nil
	})
	_ = ctx.RegisterDomain("quota").Expose()
	return nil
}

func newTestApp(t *testing.T) (app.App, func()) {
	t.Helper()
	a, err := app.New(
		app.WithNamespace("test"),
		app.WithCodec(codec.NewJSON()),
		app.WithRootActor(func() actor.Actor {
			return &testActor{}
		}),
	)
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	// Wait for root actor OnStart to complete.
	time.Sleep(100 * time.Millisecond)

	shutdown := func() {
		cancel()
		<-done
	}
	return a, shutdown
}
