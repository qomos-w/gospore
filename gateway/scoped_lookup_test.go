package gateway_test

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/gateway"
)

// scopedRoot exposes a "project" service to its descendants and records the
// spawned child actor's canonical ID so the test can drive gateway resolution.
type scopedRoot struct {
	actor.Host
	childReady chan string
}

func (a *scopedRoot) OnStart(ctx actor.Context) error {
	if err := ctx.Register("project.echo", func(_ actor.Context) (map[string]string, error) {
		return map[string]string{"msg": "pong"}, nil
	}); err != nil {
		return err
	}
	if err := ctx.RegisterDomain("project").ExposeChildren(); err != nil {
		return err
	}
	child, err := ctx.Spawn(actor.PropsFromFunc(func() actor.Actor { return &scopedChild{} }), "child")
	if err != nil {
		return err
	}
	if a.childReady != nil {
		a.childReady <- child.ID().String()
	}
	return nil
}

type scopedChild struct{ actor.Host }

func (a *scopedChild) OnStart(actor.Context) error { return nil }

func TestGatewayHTTP_ScopedServiceLookupViaFrom(t *testing.T) {
	root := &scopedRoot{childReady: make(chan string, 1)}
	_, addr, shutdown := newGatewayAppWithActor(t, gateway.Nop(), func() actor.Actor { return root })
	defer shutdown()

	var childID string
	select {
	case childID = <-root.childReady:
	case <-time.After(2 * time.Second):
		t.Fatal("child was not spawned")
	}

	url := "http://" + addr + "/api/project.echo?from=" + childID
	resp, err := http.Post(url, "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, string(body))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !strings.Contains(string(body), `"msg":"pong"`) && !strings.Contains(string(body), `"msg": "pong"`) {
		t.Fatalf("body = %q, want pong", string(body))
	}
}

func TestGatewayWS_ScopedServiceLookupViaFrom(t *testing.T) {
	root := &scopedRoot{childReady: make(chan string, 1)}
	_, addr, shutdown := newGatewayAppWithActor(t, gateway.Nop(), func() actor.Actor { return root })
	defer shutdown()

	var childID string
	select {
	case childID = <-root.childReady:
	case <-time.After(2 * time.Second):
		t.Fatal("child was not spawned")
	}

	ws := mustDialWS(t, "ws://"+addr+"/ws")
	defer ws.Close()

	mustWriteJSON(t, ws, map[string]any{
		"type":   "invoke",
		"reqId":  1,
		"callID": "project.echo",
		"from":   childID,
	})

	frame := mustReadJSON(t, ws)
	if ft, _ := frame["type"].(string); ft != "reply" {
		t.Fatalf("expected reply, got %v: %v", ft, frame)
	}
	payload, _ := frame["payload"].(map[string]any)
	if payload["msg"] != "pong" {
		t.Fatalf("payload = %v, want pong", payload)
	}
}
