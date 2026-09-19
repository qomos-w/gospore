package gateway_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/app"
	"github.com/qomos-w/gospore/codec"
	"github.com/qomos-w/gospore/gateway"
)

// ---------------------------------------------------------------------------
// Server tests
// ---------------------------------------------------------------------------

// TestGatewayHTTP_Allowed confirms a plain POST reaches the actor and
// returns the expected payload.
func TestGatewayHTTP_Allowed(t *testing.T) {
	_, addr, shutdown := newGatewayApp(t, gateway.Nop())
	defer shutdown()

	// testActor registers "test.echo" which echoes the request string.
	body, _ := json.Marshal("hello")
	resp, err := http.Post("http://"+addr+"/api/test.echo", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("http post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	data, _ := io.ReadAll(resp.Body)
	// String replies are codec-encoded with the builtin string schema, so the
	// HTTP body is the JSON representation `"hello"` (with quotes), not the
	// raw bytes.
	if string(data) != `"hello"` {
		t.Fatalf("body = %s, want \"hello\"", string(data))
	}
}

// TestGatewayHTTP_InterceptorDenies verifies a script interceptor that
// returns a non-empty string causes the gateway to reply 403.
func TestGatewayHTTP_InterceptorDenies(t *testing.T) {
	src := `import Context from "gospore"
export fun before(ctx: Context, req: any): string {
	return "quota exceeded"
}`
	interceptor, err := gateway.NewScriptInterceptor(newStandaloneApp(t), src)
	if err != nil {
		t.Fatalf("NewScriptInterceptor: %v", err)
	}

	_, addr, shutdown := newGatewayApp(t, interceptor)
	defer shutdown()

	body, _ := json.Marshal("hello")
	resp, err := http.Post("http://"+addr+"/api/test.echo", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("http post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	data, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(data), "quota exceeded") {
		t.Fatalf("body = %s, want 'quota exceeded'", string(data))
	}
}

// TestGatewayHTTP_PolicyEnforced verifies that when an HTTP request carries
// an X-Role header, the role is forwarded in Frame.Headers and the Cell's
// PolicyStore check denies the call.
func TestGatewayHTTP_PolicyEnforced(t *testing.T) {
	a, addr, shutdown := newGatewayApp(t, gateway.Nop())
	defer shutdown()

	// Load a deny-all policy for external callers with role "blocked".
	_ = a.PolicyStore().Reload([]actor.Policy{
		{Scope: "test.echo", Role: "blocked", Allow: false},
	})

	body, _ := json.Marshal("hello")
	req, _ := http.NewRequest(http.MethodPost, "http://"+addr+"/api/test.echo", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Role", "blocked")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("http do: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
	data, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(data), actor.DiagPolicyDenied) {
		t.Fatalf("body = %s, want policy denial", string(data))
	}
}

func TestGatewayHTTP_HealthEndpoint(t *testing.T) {
	_, addr, shutdown := newGatewayApp(t, gateway.Nop())
	defer shutdown()

	resp, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatalf("http get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); !strings.Contains(got, "application/json") {
		t.Fatalf("content-type = %q, want application/json", got)
	}
	data, _ := io.ReadAll(resp.Body)
	if string(bytes.TrimSpace(data)) != `{"status":"ok"}` {
		t.Fatalf("body = %s, want {\"status\":\"ok\"}", string(data))
	}
}

// TestGatewayHTTP_SchemaEndpoint verifies GET /schema returns the App's
// schema registry as JSON.
func TestGatewayHTTP_SchemaEndpoint(t *testing.T) {
	_, addr, shutdown := newGatewayApp(t, gateway.Nop())
	defer shutdown()

	resp, err := http.Get("http://" + addr + "/schema")
	if err != nil {
		t.Fatalf("http get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	data, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("invalid json response: %v", err)
	}
	if result["ns"] != "test" {
		t.Fatalf("schema namespace = %v, want test", result["ns"])
	}
}

// TestGatewayHTTP_MethodNotAllowed verifies non-POST requests to /api/
// return 405.
func TestGatewayHTTP_MethodNotAllowed(t *testing.T) {
	_, addr, shutdown := newGatewayApp(t, gateway.Nop())
	defer shutdown()

	resp, err := http.Get("http://" + addr + "/api/test.echo")
	if err != nil {
		t.Fatalf("http get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", resp.StatusCode)
	}
}

// TestGatewayHTTP_CallNotRegistered verifies that a POST to a call ID the
// root actor does not handle returns 500 with a "not registered" message.
func TestGatewayHTTP_CallNotRegistered(t *testing.T) {
	_, addr, shutdown := newGatewayApp(t, gateway.Nop())
	defer shutdown()

	body, _ := json.Marshal("hello")
	resp, err := http.Post("http://"+addr+"/api/nonexistent.call", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("http post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
	data, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(data), "not registered") {
		t.Fatalf("body = %s, want 'not registered'", string(data))
	}
}

// TestGatewayHTTP_AfterAsync confirms the After interceptor hook is called
// asynchronously after a successful call.
func TestGatewayHTTP_AfterAsync(t *testing.T) {
	afterCh := make(chan struct{}, 1)
	interceptor := &afterRecorder{afterCh: afterCh}

	_, addr, shutdown := newGatewayApp(t, interceptor)
	defer shutdown()

	body, _ := json.Marshal("hello")
	resp, err := http.Post("http://"+addr+"/api/test.echo", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("http post: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	select {
	case <-afterCh:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("After was not called within 2s")
	}
}

func TestGatewayHTTP_StaticFS(t *testing.T) {
	staticFS := fstest.MapFS{
		"dist/index.html": &fstest.MapFile{Data: []byte("<html>shell</html>")},
		"dist/assets/app.js": &fstest.MapFile{Data: []byte("console.log('ok')")},
	}
	_, addr, shutdown := newGatewayAppWithStaticFS(t, gateway.Nop(), staticFS)
	defer shutdown()

	resp, err := http.Get("http://" + addr + "/index.html")
	if err != nil {
		t.Fatalf("http get index: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("index status = %d, want 200", resp.StatusCode)
	}
	indexBody, _ := io.ReadAll(resp.Body)
	if string(indexBody) != "<html>shell</html>" {
		t.Fatalf("index body = %s", string(indexBody))
	}

	resp, err = http.Get("http://" + addr + "/projects/foo")
	if err != nil {
		t.Fatalf("http get spa route: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("spa status = %d, want 200", resp.StatusCode)
	}
	spaBody, _ := io.ReadAll(resp.Body)
	if string(spaBody) != "<html>shell</html>" {
		t.Fatalf("spa body = %s", string(spaBody))
	}

	body, _ := json.Marshal("hello")
	resp, err = http.Post("http://"+addr+"/api/test.echo", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("http post callable: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("callable status = %d, want 200", resp.StatusCode)
	}
}

// TestGatewayHTTP_TargetParam_RoutesByActorID verifies that POST /api/{callID}
// with ?target=<actorID> routes to the actor with that ID via Tree.LookupID,
// bypassing the service/path fallback.
func TestGatewayHTTP_TargetParam_RoutesByActorID(t *testing.T) {
	a, addr, shutdown := newGatewayApp(t, gateway.Nop())
	defer shutdown()

	target := a.Self().ID().String()
	body, _ := json.Marshal("hello")
	req, _ := http.NewRequest(http.MethodPost, "http://"+addr+"/api/test.echo?target="+target,
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("http do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	data, _ := io.ReadAll(resp.Body)
	// Codec-encoded string reply: JSON `"hello"` with quotes (see
	// TestGatewayHTTP_Allowed).
	if string(data) != `"hello"` {
		t.Fatalf("body = %s, want \"hello\"", string(data))
	}
}

// TestGatewayHTTP_TargetParam_ParseError verifies that a malformed target
// string (not a valid canonical ID hex) is rejected with 404 — the gateway
// must not silently fall back to the service path when a target is supplied.
func TestGatewayHTTP_TargetParam_ParseError(t *testing.T) {
	_, addr, shutdown := newGatewayApp(t, gateway.Nop())
	defer shutdown()

	body, _ := json.Marshal("hello")
	req, _ := http.NewRequest(http.MethodPost, "http://"+addr+"/api/test.echo?target=not-an-id",
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("http do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// TestGatewayHTTP_TargetParam_UnknownID verifies that a syntactically valid
// but unregistered ID returns 404 rather than silently falling back to a
// service-based target — misrouting must surface, not be masked.
func TestGatewayHTTP_TargetParam_UnknownID(t *testing.T) {
	_, addr, shutdown := newGatewayApp(t, gateway.Nop())
	defer shutdown()

	// 32-character hex but not pointing at any actor in this app.
	unknownID := "00000000000000000000000000000001"
	body, _ := json.Marshal("hello")
	req, _ := http.NewRequest(http.MethodPost, "http://"+addr+"/api/test.echo?target="+unknownID,
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("http do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}


// afterRecorder is a GatewayInterceptor that records After invocations.
type afterRecorder struct {
	afterCh chan struct{}
}

func (a *afterRecorder) Before(ctx context.Context, req *gateway.GatewayRequest) error { return nil }
func (a *afterRecorder) After(ctx context.Context, req *gateway.GatewayRequest, resp *gateway.GatewayResponse) {
	a.afterCh <- struct{}{}
}

// newStandaloneApp creates an App without starting it. Used when an
// interceptor needs an App reference before the server is spun up.
func newStandaloneApp(t *testing.T) app.App {
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
	return a
}

// newGatewayApp creates an App, starts it, spins up a Gateway server on an
// ephemeral port, and returns the App, the server's HTTP address, and a
// shutdown function.
func newGatewayApp(t *testing.T, interceptor gateway.GatewayInterceptor) (app.App, string, func()) {
	return newGatewayAppWithStaticFS(t, interceptor, nil)
}

func newGatewayAppWithStaticFS(t *testing.T, interceptor gateway.GatewayInterceptor, staticFS fs.FS) (app.App, string, func()) {
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

	// Start gateway server manually so we can capture its address.
	srv := gateway.NewServer(a, interceptor, "127.0.0.1:0")
	srv.SetStaticFS(staticFS)
	go func() { _ = srv.Run(ctx) }()

	// Wait for listener to be active.
	for srv.Addr() == "" {
		time.Sleep(10 * time.Millisecond)
	}

	shutdown := func() {
		cancel()
		<-done
	}
	return a, srv.Addr(), shutdown
}
