package gateway

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/qomos-w/gospore/ref"
	"github.com/qomos-w/gospore/schema"
)

// holdConn blocks Recv until Close, mimicking an idle connected client.
type holdConn struct {
	blocked chan struct{}
}

func newHoldConn() *holdConn { return &holdConn{blocked: make(chan struct{})} }

func (c *holdConn) Recv() (*WireFrame, error) {
	<-c.blocked
	return nil, errors.New("closed")
}

func (c *holdConn) Send(*WireFrame) error { return nil }

func (c *holdConn) Close() error {
	select {
	case <-c.blocked:
	default:
		close(c.blocked)
	}
	return nil
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for !cond() {
		select {
		case <-deadline:
			t.Fatal("condition not met within 3s")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestActiveSessionsLifecycle pins register-on-start / remove-on-exit and
// the snapshot content of a pre-authenticated session.
func TestActiveSessionsLifecycle(t *testing.T) {
	srv := &Server{}
	conn := newHoldConn()
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.ServeSession(context.Background(), conn, SessionOptions{Role: "admin", Subject: "admin"})
	}()

	waitFor(t, func() bool { return len(srv.ActiveSessions()) == 1 })
	sessions := srv.ActiveSessions()
	if sessions[0].Kind != "ws" || sessions[0].Role != "admin" || sessions[0].Subject != "admin" {
		t.Fatalf("unexpected session info: %+v", sessions[0])
	}
	if sessions[0].StartedAt.IsZero() {
		t.Fatal("StartedAt not set")
	}

	conn.Close()
	<-done
	if got := srv.ActiveSessions(); len(got) != 0 {
		t.Fatalf("session not removed after disconnect: %+v", got)
	}
}

// TestActiveSessionsMultipleConcurrent verifies several live sessions are
// reported independently and removed independently.
func TestActiveSessionsMultipleConcurrent(t *testing.T) {
	srv := &Server{}
	conns := []*holdConn{newHoldConn(), newHoldConn()}
	done := make([]chan struct{}, len(conns))
	for i, conn := range conns {
		done[i] = make(chan struct{})
		go func(i int, conn *holdConn) {
			defer close(done[i])
			srv.ServeSession(context.Background(), conn, SessionOptions{
				Role:    "developer",
				Subject: []string{"user-a", "user-b"}[i],
			})
		}(i, conn)
	}

	waitFor(t, func() bool { return len(srv.ActiveSessions()) == 2 })
	subjects := map[string]bool{}
	for _, s := range srv.ActiveSessions() {
		subjects[s.Subject] = true
	}
	if !subjects["user-a"] || !subjects["user-b"] {
		t.Fatalf("expected both subjects, got %+v", srv.ActiveSessions())
	}

	conns[0].Close()
	<-done[0]
	waitFor(t, func() bool { return len(srv.ActiveSessions()) == 1 })
	if srv.ActiveSessions()[0].Subject != "user-b" {
		t.Fatalf("wrong session survived: %+v", srv.ActiveSessions())
	}

	conns[1].Close()
	<-done[1]
	if got := srv.ActiveSessions(); len(got) != 0 {
		t.Fatalf("sessions not drained: %+v", got)
	}
}

// authScriptConn delivers one pre-queued wire frame (the auth frame), then
// blocks like an idle connected client.
type authScriptConn struct {
	holdConn
	queued *WireFrame
	once   sync.Once
}

func (c *authScriptConn) Recv() (*WireFrame, error) {
	var frame *WireFrame
	c.once.Do(func() { frame, c.queued = c.queued, nil })
	if frame != nil {
		return frame, nil
	}
	return c.holdConn.Recv()
}

// authOnlyApp stubs the two AppHost methods handleAuthFrame touches.
type authOnlyApp struct{ AppHost }

func (authOnlyApp) ManifestJSON() []byte { return []byte("{}") }
func (authOnlyApp) ExportGosporeManifest() (schema.GosporeManifest, error) {
	return schema.GosporeManifest{}, errors.New("not implemented")
}
func (authOnlyApp) SpawnGatewaySession(name string) (ref.Ref, error) {
	return nil, errors.New("not implemented")
}

// TestActiveSessionsAuthFrameUpgrade pins that a session which starts
// anonymous and later authenticates via an auth frame has its registry
// identity rewritten in place.
func TestActiveSessionsAuthFrameUpgrade(t *testing.T) {
	srv := &Server{app: authOnlyApp{}}
	conn := &authScriptConn{holdConn: holdConn{blocked: make(chan struct{})}}
	conn.queued = &WireFrame{
		Type:    FrameTypeAuth,
		Flags:   MakeFlags(EncodingJSON, CompressionNone),
		Payload: []byte("tok-123"),
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.ServeSession(context.Background(), conn, SessionOptions{
			URLAuth: func(ctx context.Context, q url.Values, app AppHost) (string, string, error) {
				if token := q.Get("token"); token != "" {
					return "developer", "user-a", nil
				}
				return "anonymous", "", nil
			},
		})
	}()

	waitFor(t, func() bool {
		sessions := srv.ActiveSessions()
		return len(sessions) == 1 && sessions[0].Role == "developer" && sessions[0].Subject == "user-a"
	})

	conn.Close()
	<-done
	if got := srv.ActiveSessions(); len(got) != 0 {
		t.Fatalf("sessions not drained: %+v", got)
	}
}

// TestSessionRegistryUpdateIdentityPins pins the in-place identity rewrite
// semantics of updateIdentity directly.
func TestSessionRegistryUpdateIdentityPins(t *testing.T) {
	var r sessionRegistry
	e := r.add(SessionInfo{Kind: "ws", Role: "anonymous", Subject: "", StartedAt: time.Now()})
	r.updateIdentity(e, "developer", "user-a")

	snap := r.snapshot()
	if len(snap) != 1 || snap[0].Role != "developer" || snap[0].Subject != "user-a" {
		t.Fatalf("identity not rewritten: %+v", snap)
	}

	// Removing a foreign/unknown entry is a no-op; the real one survives.
	r.remove(&sessionEntry{})
	if len(r.snapshot()) != 1 {
		t.Fatal("unknown entry removal dropped the live session")
	}

	r.remove(e)
	if len(r.snapshot()) != 0 {
		t.Fatal("entry not removed")
	}

	// Updates after removal are dropped, not resurrected.
	r.updateIdentity(e, "admin", "admin")
	if len(r.snapshot()) != 0 {
		t.Fatal("update resurrected a removed entry")
	}
}
