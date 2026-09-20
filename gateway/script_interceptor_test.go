package gateway_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/app"
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
// can lookup a service actor and invoke it, and that a failed invoke is a
// catchable script error (not a silent nil).
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
import InvokeResult from "gospore"
export fun before(ctx: Context, req: any): string {
	var quota: ActorRef = ctx.lookup_service("quota")
	var result: InvokeResult = quota.invoke("check", {})
	if (result.err != "") {
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
	} else if !strings.Contains(err.Error(), "quota check failed") {
		t.Fatalf("Before: unexpected error: %v", err)
	}
}

// TestScriptInterceptor_InvokeSuccess verifies a successful actor invoke
// returns its value to the script (non-nil), allowing the request through.
func TestScriptInterceptor_InvokeSuccess(t *testing.T) {
	a, shutdown := newTestApp(t)
	defer shutdown()

	_, err := a.Spawn(actor.PropsFromFunc(func() actor.Actor {
		return &slowActor{delay: 0}
	}), "slow")
	if err != nil {
		t.Fatalf("Spawn slow: %v", err)
	}

	src := `import Context from "gospore"
import ActorRef from "gospore"
import InvokeResult from "gospore"
export fun before(ctx: Context, req: any): string {
	var svc: ActorRef = ctx.lookup_service("slow")
	var result: InvokeResult = svc.invoke("test.slow", {})
	if (result.err != "") {
		return result.err
	}
	if (result.value == null) {
		return "slow check returned null"
	}
	return ""
}`
	si, err := gateway.NewScriptInterceptor(a, src)
	if err != nil {
		t.Fatalf("NewScriptInterceptor: %v", err)
	}

	req := &gateway.GatewayRequest{CallID: "test.echo", CustomerID: "alice"}
	if err := si.Before(context.Background(), req); err != nil {
		t.Fatalf("Before: expected allow, got %v", err)
	}
}

// TestScriptInterceptor_InvokeValueShape pins the cross-host value
// contract: a reply payload that arrives on the wire as raw JSON bytes
// must reach the script as a STRING (the codec-configured cell-host
// normalization, cell.ScriptNormalizeBytes). Before the shared
// normalizer the gateway handed raw bytes to scripts while the cell
// host returned a string — same callable, different payload shape
// depending on which host executed the script.
func TestScriptInterceptor_InvokeValueShape(t *testing.T) {
	a, shutdown := newTestApp(t)
	defer shutdown()

	_, err := a.Spawn(actor.PropsFromFunc(func() actor.Actor {
		return &shapeActor{}
	}), "shape")
	if err != nil {
		t.Fatalf("Spawn shape: %v", err)
	}

	src := `import Context from "gospore"
import ActorRef from "gospore"
import InvokeResult from "gospore"
export fun before(ctx: Context, req: any): string {
	var svc: ActorRef = ctx.lookup_service("shape")
	var result: InvokeResult = svc.invoke("shape.get", {})
	if (result.err != "") {
		return result.err
	}
	if (result.value.v != 7) {
		return "wrong value shape"
	}
	return ""
}`
	si, err := gateway.NewScriptInterceptor(a, src)
	if err != nil {
		t.Fatalf("NewScriptInterceptor: %v", err)
	}

	req := &gateway.GatewayRequest{CallID: "test.echo", CustomerID: "alice"}
	if err := si.Before(context.Background(), req); err != nil {
		t.Fatalf("Before: value shape contract violated: %v", err)
	}
}

type shapeActor struct {
	actor.Host
}

func (a *shapeActor) OnStart(ctx actor.Context) error {
	if err := ctx.Register("shape.get", func(_ actor.Context, _ shapeReq) (shapeResp, error) {
		return shapeResp{V: 7}, nil
	}); err != nil {
		return err
	}
	return ctx.RegisterDomain("shape").Expose()
}

type shapeReq struct {
	Ignore bool `json:"ignore"`
}

type shapeResp struct {
	V int `json:"v"`
}

// TestScriptInterceptor_InvokeErrorPropagates verifies a failed invoke
// reports its error through InvokeResult.err, which the script can turn
// into a denial message.
func TestScriptInterceptor_InvokeErrorPropagates(t *testing.T) {
	a, shutdown := newTestApp(t)
	defer shutdown()

	src := `import Context from "gospore"
import ActorRef from "gospore"
import InvokeResult from "gospore"
export fun before(ctx: Context, req: any): string {
	var svc: ActorRef = ctx.lookup_service("does-not-exist")
	var result: InvokeResult = svc.invoke("test.slow", {})
	if (result.err != "") {
		return result.err
	}
	return ""
}`
	si, err := gateway.NewScriptInterceptor(a, src)
	if err != nil {
		t.Fatalf("NewScriptInterceptor: %v", err)
	}

	req := &gateway.GatewayRequest{CallID: "test.echo", CustomerID: "alice"}
	err = si.Before(context.Background(), req)
	if err == nil {
		t.Fatal("Before: expected invoke error, got nil")
	}
	if !strings.Contains(err.Error(), "invoke") {
		t.Fatalf("Before: error should mention the failed invoke, got %v", err)
	}
}

// TestScriptInterceptor_ConcurrentSlowInvokes verifies concurrent Before
// calls whose scripts each block on a slow actor invoke do not serialize
// behind a single runtime lock: with interceptorPoolSize slots the wall
// time must be well below N × delay.
func TestScriptInterceptor_ConcurrentSlowInvokes(t *testing.T) {
	a, shutdown := newTestApp(t)
	defer shutdown()

	const delay = 250 * time.Millisecond
	_, err := a.Spawn(actor.PropsFromFunc(func() actor.Actor {
		return &slowActor{delay: delay}
	}), "slow")
	if err != nil {
		t.Fatalf("Spawn slow: %v", err)
	}

	src := `import Context from "gospore"
import ActorRef from "gospore"
export fun before(ctx: Context, req: any): string {
	var svc: ActorRef = ctx.lookup_service("slow")
	svc.invoke("test.slow", {})
	return ""
}`
	si, err := gateway.NewScriptInterceptor(a, src)
	if err != nil {
		t.Fatalf("NewScriptInterceptor: %v", err)
	}

	req := &gateway.GatewayRequest{CallID: "test.echo", CustomerID: "alice"}
	const n = 8
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := si.Before(context.Background(), req); err != nil {
				t.Errorf("Before: %v", err)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	// Serialized execution would take >= n*delay = 2s; pooled execution
	// lands near one delay. 1.5s leaves generous headroom on slow CI.
	if elapsed > 1500*time.Millisecond {
		t.Fatalf("Before calls serialized: %d invocations of %v took %v", n, delay, elapsed)
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

// slowActor exposes "test.slow" as a stateless handler so concurrent
// invocations fork instead of serializing on the actor's cell goroutine.
// The typed-struct param exercises the script-adapter decode path: it
// requires a resolver-bound codec (the app default), which is exactly
// the wiring this file's newTestApp now pins.
type slowReq struct {
	Q string `json:"q"`
}

type slowActor struct {
	actor.Host
	delay time.Duration
}

func (a *slowActor) OnStart(ctx actor.Context) error {
	_ = ctx.Register("test.slow", func(_ actor.PureContext, _ slowReq) (map[string]any, error) {
		if a.delay > 0 {
			time.Sleep(a.delay)
		}
		return map[string]any{"ok": true}, nil
	})
	_ = ctx.RegisterDomain("slow").Expose()
	return nil
}

func newTestApp(t *testing.T) (app.App, func()) {
	t.Helper()
	a, err := app.New(
		app.WithNamespace("test"),
		// Default codec (NewMulti bound to the app SchemaSet). A bare
		// codec.NewJSON() has no schema resolver and cannot decode
		// typed-struct payloads receiver-side.
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
