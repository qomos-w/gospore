package integration

import (
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/app"
	"github.com/qomos-w/gospore/schema"
	"github.com/qomos-w/gospore/transport"
	spore "github.com/qomos-w/spore/schema"
)

// ---- typed payloads (mirror the exp09 auth namespace) -------------------

type LoginReq struct {
	User string `json:"user"`
}

type LoginResp struct {
	Ok    bool   `json:"ok"`
	Token string `json:"token"`
}

type TailLoginsReq struct {
	Limit int `json:"limit"`
}

type LoginEvent struct {
	User string `json:"user"`
	At   string `json:"at"`
}

type TailLoginsFinal struct {
	Total int `json:"total"`
}

// authActor is the server-side actor that exposes typed unary and streaming
// handlers. It demonstrates the Spore full-stack integration pattern:
// typed Go methods → gospore handler registration → cross-App invocation.
type authActor struct {
	actor.Host
	events []LoginEvent
}

func (a *authActor) OnStart(ctx actor.Context) error {
	_ = ctx.Register("auth.login", func(_ actor.Context, req LoginReq) (LoginResp, error) {
		return LoginResp{Ok: true, Token: "tok-" + req.User}, nil
	})
	_ = ctx.Register("auth.tail_logins", func(_ actor.Context, req TailLoginsReq, em actor.Emitter) error {
		limit := req.Limit
		if limit <= 0 || limit > len(a.events) {
			limit = len(a.events)
		}
		for i := 0; i < limit; i++ {
			if err := em.Send(a.events[i]); err != nil {
				return err
			}
		}
		return nil
	})
	return nil
}

// awaitStarted sleeps briefly to let OnStart finish. In production code
// this would use Events.Subscribe(WatchStarted); here we keep the test
// minimal.
func awaitStarted() { time.Sleep(100 * time.Millisecond) }

// TestSporeFullStack_Unary confirms a typed unary handler can be invoked
// across two Apps via HTTP transport, with the request/response travelling
// through message.Frame + invoke.Call.
func TestSporeFullStack_Unary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	remoteA, err := transport.NewHTTP("")
	if err != nil {
		t.Fatalf("NewHTTP A: %v", err)
	}
	defer remoteA.Close()

	remoteB, err := transport.NewHTTP("")
	if err != nil {
		t.Fatalf("NewHTTP B: %v", err)
	}
	defer remoteB.Close()

	// Router resolves runtime slots to peer addresses automatically.
	router := transport.NewStaticRouter()
	router.Register(1, "http://"+remoteA.ListenAddr())
	router.Register(2, "http://"+remoteB.ListenAddr())
	remoteA.SetRouter(router)
	remoteB.SetRouter(router)

	// Server App (slot 2) hosts the auth actor.
	server, err := app.New(
		app.WithNamespace("auth"),
		app.WithRuntimeSlot(2),
		app.WithRemoteTransport(remoteB),
		app.WithRootActor(func() actor.Actor {
			return &authActor{}
		}),
	)
	if err != nil {
		t.Fatalf("New server: %v", err)
	}

	// Client App (slot 1) has no local handlers; it calls the server remotely.
	client, err := app.New(
		app.WithNamespace("auth"),
		app.WithRuntimeSlot(1),
		app.WithRemoteTransport(remoteA),
	)
	if err != nil {
		t.Fatalf("New client: %v", err)
	}

	doneS := make(chan error, 1)
	doneC := make(chan error, 1)
	go func() { doneS <- server.Run(ctx) }()
	go func() { doneC <- client.Run(ctx) }()
	awaitStarted()

	// ---- unary call via RemoteRef ----
	// Cross-process calls require manual JSON encoding because the client
	// cannot resolve the server's handler metadata locally.
	reqBody, _ := json.Marshal(LoginReq{User: "alice"})
	refServer := client.RemoteRef(server.Self().ID())
	call := refServer.Invoke(ctx, "auth.login", reqBody)
	if call == nil {
		t.Fatal("Invoke returned nil call")
	}

	val, err := call.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}

	var resp LoginResp
	switch v := val.(type) {
	case LoginResp:
		resp = v
	case []byte:
		if err := json.Unmarshal(v, &resp); err != nil {
			t.Fatalf("unmarshal resp: %v", err)
		}
	default:
		raw, _ := json.Marshal(v)
		if err := json.Unmarshal(raw, &resp); err != nil {
			t.Fatalf("unmarshal resp: %v", err)
		}
	}
	if !resp.Ok {
		t.Fatalf("resp.Ok = false, want true")
	}
	if resp.Token != "tok-alice" {
		t.Fatalf("resp.Token = %q, want tok-alice", resp.Token)
	}

	cancel()
	<-doneS
	<-doneC
}

// TestSporeFullStack_Streaming confirms a typed streaming handler can be
// invoked across two Apps via HTTP transport, with chunks and the final End
// frame travelling through message.Frame + invoke.Call.
func TestSporeFullStack_Streaming(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	remoteA, err := transport.NewHTTP("")
	if err != nil {
		t.Fatalf("NewHTTP A: %v", err)
	}
	defer remoteA.Close()

	remoteB, err := transport.NewHTTP("")
	if err != nil {
		t.Fatalf("NewHTTP B: %v", err)
	}
	defer remoteB.Close()

	router := transport.NewStaticRouter()
	router.Register(1, "http://"+remoteA.ListenAddr())
	router.Register(2, "http://"+remoteB.ListenAddr())
	remoteA.SetRouter(router)
	remoteB.SetRouter(router)

	fixtureEvents := []LoginEvent{
		{User: "alice", At: "2026-01-01T00:00:00Z"},
		{User: "bob", At: "2026-01-02T00:00:00Z"},
		{User: "carol", At: "2026-01-03T00:00:00Z"},
	}

	server, err := app.New(
		app.WithNamespace("auth"),
		app.WithRuntimeSlot(2),
		app.WithRemoteTransport(remoteB),
		app.WithRootActor(func() actor.Actor {
			return &authActor{events: fixtureEvents}
		}),
	)
	if err != nil {
		t.Fatalf("New server: %v", err)
	}

	client, err := app.New(
		app.WithNamespace("auth"),
		app.WithRuntimeSlot(1),
		app.WithRemoteTransport(remoteA),
	)
	if err != nil {
		t.Fatalf("New client: %v", err)
	}

	doneS := make(chan error, 1)
	doneC := make(chan error, 1)
	go func() { doneS <- server.Run(ctx) }()
	go func() { doneC <- client.Run(ctx) }()
	awaitStarted()

	// Cross-process streaming calls require manual JSON encoding.
	reqBody, _ := json.Marshal(TailLoginsReq{Limit: 2})
	refServer := client.RemoteRef(server.Self().ID())
	call := refServer.Invoke(ctx, "auth.tail_logins", reqBody)
	if call == nil {
		t.Fatal("Invoke returned nil call")
	}

	// Consume two Reply chunks via Recv().
	var got []LoginEvent
	for i := 0; i < 2; i++ {
		v, err := call.Recv()
		if err != nil {
			t.Fatalf("Recv chunk %d: %v", i, err)
		}
		var ev LoginEvent
		switch rv := v.(type) {
		case LoginEvent:
			ev = rv
		case []byte:
			if err := json.Unmarshal(rv, &ev); err != nil {
				t.Fatalf("unmarshal chunk %d: %v", i, err)
			}
		default:
			raw, _ := json.Marshal(rv)
			if err := json.Unmarshal(raw, &ev); err != nil {
				t.Fatalf("unmarshal chunk %d: %v", i, err)
			}
		}
		got = append(got, ev)
	}

	// After the handler returns nil, the End frame yields io.EOF.
	_, err = call.Recv()
	if err != io.EOF {
		t.Fatalf("expected io.EOF after last chunk, got %v", err)
	}

	// Verify chunk content.
	if len(got) != 2 {
		t.Fatalf("got %d chunks, want 2", len(got))
	}
	if got[0].User != "alice" {
		t.Fatalf("chunk[0].User = %q, want alice", got[0].User)
	}
	if got[1].User != "bob" {
		t.Fatalf("chunk[1].User = %q, want bob", got[1].User)
	}

	cancel()
	<-doneS
	<-doneC
}

// TestSporeFullStack_ManifestImport validates that manifest schemas are
// correctly imported into the client SchemaSet and round-trip through
// SchemaSet lookup.
func TestSporeFullStack_ManifestImport(t *testing.T) {
	decls := []spore.NamespaceDecl{{
		Namespace: "auth",
		Methods:   schema.MethodsOf(&authActor{}),
	}}
	streaming := []schema.StreamingDef{{
		Namespace:    "auth",
		CallableName: "tail_logins",
		ReqType:      TailLoginsReq{},
		ChunkType:    LoginEvent{},
		FinalType:    TailLoginsFinal{},
	}}
	manifest, err := schema.BuildManifestWithStreaming(decls, streaming)
	if err != nil {
		t.Fatalf("BuildManifestWithStreaming: %v", err)
	}

	// Import into a fresh SchemaSet.
	set, err := schema.New("auth")
	if err != nil {
		t.Fatalf("schema.New: %v", err)
	}
	if err := schema.ImportFromManifest(set, manifest); err != nil {
		t.Fatalf("ImportFromManifest: %v", err)
	}

	// Verify that at least the streaming schemas were imported.
	if len(manifest.Schemas) == 0 {
		t.Fatal("manifest has no schemas")
	}
	for _, ms := range manifest.Schemas {
		entry, ok := set.Lookup(ms.SchemaID)
		if !ok {
			t.Fatalf("schema %d (%s) not found in set", ms.SchemaID, ms.Name)
		}
		if entry.Name != ms.Name {
			t.Fatalf("schema %d: name = %q, want %q", ms.SchemaID, entry.Name, ms.Name)
		}
	}
}
