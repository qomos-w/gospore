package gateway_test

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/app"
	"github.com/qomos-w/gospore/codec"
	"github.com/qomos-w/gospore/gateway"
)

type urlAuthProbeActor struct{ actor.Host }

func (a *urlAuthProbeActor) OnStart(ctx actor.Context) error {
	_ = ctx.Register("test.ping", func(_ actor.Context, _ string) (map[string]string, error) {
		return map[string]string{"pong": "1"}, nil
	})
	return nil
}

// A token that fails URLAuth must fail fast: the server closes the socket
// with the private close code 4401 instead of proceeding as an anonymous
// session (which turns client claims into an undetectable auth-error loop).
func TestWSURLAuthInvalidTokenCloses4401(t *testing.T) {
	a, err := app.New(
		app.WithNamespace("test"),
		app.WithCodec(codec.NewJSON()),
		app.WithRootActor(func() actor.Actor { return &urlAuthProbeActor{} }),
	)
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)

	srv := gateway.NewServer(a, nil, "127.0.0.1:0")
	srv.WithURLAuth(func(_ context.Context, q url.Values, _ gateway.AppHost) (string, string, error) {
		if tok := q.Get("token"); tok != "" && tok != "good" {
			return "", "", errors.New("invalid token")
		}
		return "anonymous", "", nil
	})
	go func() { _ = srv.Run(ctx) }()
	for srv.Addr() == "" {
		time.Sleep(10 * time.Millisecond)
	}

	// Presented-but-invalid token: expect close 4401.
	ws, _, err := websocket.DefaultDialer.Dial("ws://"+srv.Addr()+"/ws?token=bad", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer ws.Close()
	ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err = ws.ReadMessage()
	ce, ok := err.(*websocket.CloseError)
	if !ok {
		t.Fatalf("read: want CloseError, got %v", err)
	}
	if ce.Code != 4401 {
		t.Fatalf("close code = %d, want 4401", ce.Code)
	}

	// No token: connection proceeds anonymous (no immediate close).
	ws2, _, err := websocket.DefaultDialer.Dial("ws://"+srv.Addr()+"/ws", nil)
	if err != nil {
		t.Fatalf("dial anonymous: %v", err)
	}
	defer ws2.Close()
	ws2.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if _, _, err := ws2.ReadMessage(); err != nil {
		if ce, ok := err.(*websocket.CloseError); ok && ce.Code == 4401 {
			t.Fatal("token-less connection must not be closed with 4401")
		}
	}
}
