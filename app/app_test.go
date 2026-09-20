package app

import (
	"context"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/discovery"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/message"
	"github.com/qomos-w/gospore/ref"
	"github.com/qomos-w/gospore/transport"
)

func TestGatewayReadyAvailableBeforeRun(t *testing.T) {
	a, err := New(
		WithNamespace("test"),
		WithGatewayHTTP("127.0.0.1:0", nil),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ready := a.(*appImpl).GatewayReady()
	if ready == nil {
		t.Fatal("GatewayReady returned nil before Run")
	}
	select {
	case <-ready:
		t.Fatal("gateway reported ready before Run")
	default:
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("gateway did not become ready")
	}
	cancel()
	<-done
}

// TestApp_RemoteTransport_ReceiveLoop confirms inbound frames on the
// remote transport are delivered to local actors by the receive loop.
// The root actor has no handler registered, so it replies with an error
// which is routed back through the remote transport.
func TestApp_RemoteTransport_ReceiveLoop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	remoteA, remoteB := transport.NewMemoryRemotePair()

	a, err := New(
		WithNamespace("testa"),
		WithRuntimeSlot(1),
		WithRemoteTransport(remoteA),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	// Wait for the App to start.
	time.Sleep(50 * time.Millisecond)

	// Send a frame from remoteB into App A's receive loop.
	// MemoryRemote is bidirectional: remoteB.Send delivers to remoteA.Receive.
	frame := message.Frame{
		From:   id.ActorID{}, // remote sender
		To:     a.Self().ID(),
		Kind:   message.KindCall,
		CorID:  1,
		CallID: "test.ping",
		Body:   []byte("hello"),
	}
	if err := remoteB.Send(frame); err != nil {
		t.Fatalf("remoteB.Send: %v", err)
	}

	// The root actor has no handler, so it replies with an Error frame
	// followed by End. Replies go out via remoteA.Send, arriving at
	// remoteB.Receive.
	for i := 0; i < 2; i++ {
		select {
		case recv := <-remoteB.Receive():
			if i == 0 && recv.Kind != message.KindError {
				t.Errorf("first reply: got Kind=%v, want KindError", recv.Kind)
			}
			if i == 1 && recv.Kind != message.KindEnd {
				t.Errorf("second reply: got Kind=%v, want KindEnd", recv.Kind)
			}
			if recv.CorID != 1 {
				t.Errorf("reply CorID: got %d, want 1", recv.CorID)
			}
		case <-ctx.Done():
			t.Fatalf("timeout waiting for reply %d", i)
		}
	}

	cancel()
	<-done
}

// TestApp_RemoteTransport_DeliverRoutesToRemote confirms deliver sends
// to the remote transport when the target is not in the local registry.
func TestApp_RemoteTransport_DeliverRoutesToRemote(t *testing.T) {
	remoteA, remoteB := transport.NewMemoryRemotePair()

	a, err := New(
		WithNamespace("testa"),
		WithRuntimeSlot(1),
		WithRemoteTransport(remoteA),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	impl := a.(*appImpl)

	// Create a fake remote ref (not in local registry).
	remoteAID := id.ActorID{}
	fakeRef := &fakeRef{aid: remoteAID}

	frame := message.Frame{
		From:   impl.rootRef.ID(),
		To:     remoteAID,
		Kind:   message.KindCall,
		CorID:  42,
		CallID: "remote.echo",
		Body:   []byte("ping"),
	}
	env := mailbox.Envelope{Frame: frame}

	if err := impl.deliver(fakeRef, env); err != nil {
		t.Fatalf("deliver to remote: %v", err)
	}

	// The frame should arrive on remoteB's Receive channel.
	select {
	case recv := <-remoteB.Receive():
		if recv.CallID != "remote.echo" {
			t.Errorf("CallID: got %q, want %q", recv.CallID, "remote.echo")
		}
		if recv.CorID != 42 {
			t.Errorf("CorID: got %d, want 42", recv.CorID)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for frame on remoteB")
	}
}

type fakeRef struct {
	aid id.ActorID
}

func (f *fakeRef) ID() id.ActorID                            { return f.aid }
func (f *fakeRef) Service() (string, bool)                   { return "", false }
func (f *fakeRef) Invoke(_ context.Context, _ string, _ any, _ ...map[string]string) *invoke.Call { return nil }

var _ ref.Ref = (*fakeRef)(nil)

// echoActor registers a "test.echo" handler that echoes the payload.
type echoActor struct {
	actor.Host
}

func (a *echoActor) OnStart(ctx actor.Context) error {
	if err := a.Host.OnStart(ctx); err != nil {
		return err
	}
	return ctx.Register("test.echo", func(_ actor.Context, req []byte) ([]byte, error) {
		return req, nil
	})
}

// serviceActor registers a handler and exposes it to discovery.
type serviceActor struct {
	actor.Host
	callID      string
	serviceName string
}

func (a *serviceActor) OnStart(ctx actor.Context) error {
	if err := a.Host.OnStart(ctx); err != nil {
		return err
	}
	if err := ctx.Register(a.callID, func(_ actor.Context, req []byte) ([]byte, error) {
		return req, nil
	}); err != nil {
		return err
	}
	return ctx.RegisterDomain(a.serviceName).Expose()
}

// watchRootActor watches a target ref and signals on terminated when it
// receives OnTerminated for that exact target.
type watchRootActor struct {
	actor.Host
	targetRef  ref.Ref
	terminated chan struct{}
}

func (a *watchRootActor) OnStart(ctx actor.Context) error {
	if err := a.Host.OnStart(ctx); err != nil {
		return err
	}
	if a.targetRef == nil {
		return nil
	}
	return ctx.Watch(a.targetRef)
}

func (a *watchRootActor) OnTerminated(ctx actor.Context, of ref.Ref) {
	if of != nil && a.targetRef != nil && of.ID() == a.targetRef.ID() {
		select {
		case a.terminated <- struct{}{}:
		default:
		}
	}
}

// TestApp_CrossApp_Unary confirms two Apps wired by MemoryRemote can
// exchange a unary call and reply end-to-end via appImpl.invoke.
func TestApp_CrossApp_Unary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	remoteA, remoteB := transport.NewMemoryRemotePair()

	// App A with echo handler.
	a, err := New(
		WithNamespace("appa"),
		WithRuntimeSlot(1),
		WithRemoteTransport(remoteA),
		WithRootActor(func() actor.Actor { return &echoActor{} }),
	)
	if err != nil {
		t.Fatalf("New A: %v", err)
	}

	// App B with echo handler.
	b, err := New(
		WithNamespace("appb"),
		WithRuntimeSlot(2),
		WithRemoteTransport(remoteB),
		WithRootActor(func() actor.Actor { return &echoActor{} }),
	)
	if err != nil {
		t.Fatalf("New B: %v", err)
	}

	doneA := make(chan error, 1)
	doneB := make(chan error, 1)
	go func() { doneA <- a.Run(ctx) }()
	go func() { doneB <- b.Run(ctx) }()

	// Wait for both Apps to start.
	time.Sleep(100 * time.Millisecond)

	// App A calls App B's root actor via RemoteRef transparent routing.
	refB := a.RemoteRef(b.Self().ID())
	call := refB.Invoke(ctx, "test.echo", []byte("hello-cross"))
	val, err := call.Recv()
	if err != nil {
		t.Fatalf("A->B call error: %v", err)
	}
	if string(val.([]byte)) != "hello-cross" {
		t.Fatalf("A->B result: got %q, want hello-cross", val)
	}

	// App B calls App A's root actor via RemoteRef transparent routing.
	refA := b.RemoteRef(a.Self().ID())
	call2 := refA.Invoke(ctx, "test.echo", []byte("world-cross"))
	val2, err2 := call2.Recv()
	if err2 != nil {
		t.Fatalf("B->A call error: %v", err2)
	}
	if string(val2.([]byte)) != "world-cross" {
		t.Fatalf("B->A result: got %q, want world-cross", val2)
	}

	cancel()
	<-doneA
	<-doneB
}

// TestApp_CrossApp_HTTP confirms two Apps wired by transport.HTTP can
// exchange a unary call end-to-end over real localhost HTTP.
func TestApp_CrossApp_HTTP(t *testing.T) {
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

	// Wire the transports to each other.
	remoteA.SetTargetURL("http://" + remoteB.ListenAddr())
	remoteB.SetTargetURL("http://" + remoteA.ListenAddr())

	// App A with echo handler.
	a, err := New(
		WithNamespace("appa"),
		WithRuntimeSlot(1),
		WithRemoteTransport(remoteA),
		WithRootActor(func() actor.Actor { return &echoActor{} }),
	)
	if err != nil {
		t.Fatalf("New A: %v", err)
	}

	// App B with echo handler.
	b, err := New(
		WithNamespace("appb"),
		WithRuntimeSlot(2),
		WithRemoteTransport(remoteB),
		WithRootActor(func() actor.Actor { return &echoActor{} }),
	)
	if err != nil {
		t.Fatalf("New B: %v", err)
	}

	doneA := make(chan error, 1)
	doneB := make(chan error, 1)
	go func() { doneA <- a.Run(ctx) }()
	go func() { doneB <- b.Run(ctx) }()

	time.Sleep(100 * time.Millisecond)

	// App A calls App B's root actor over HTTP via RemoteRef.
	refB := a.RemoteRef(b.Self().ID())
	call := refB.Invoke(ctx, "test.echo", []byte("hello-http"))
	val, err := call.Recv()
	if err != nil {
		t.Fatalf("A->B call error: %v", err)
	}
	if string(val.([]byte)) != "hello-http" {
		t.Fatalf("A->B result: got %q, want hello-http", val)
	}

	// App B calls App A's root actor over HTTP via RemoteRef.
	refA := b.RemoteRef(a.Self().ID())
	call2 := refA.Invoke(ctx, "test.echo", []byte("world-http"))
	val2, err2 := call2.Recv()
	if err2 != nil {
		t.Fatalf("B->A call error: %v", err2)
	}
	if string(val2.([]byte)) != "world-http" {
		t.Fatalf("B->A result: got %q, want world-http", val2)
	}

	cancel()
	<-doneA
	<-doneB
}

// TestApp_CrossApp_HTTP_WithRouter confirms two Apps wired by HTTP
// transports with a StaticRouter can exchange unary calls without
// manual SetTargetURL.
func TestApp_CrossApp_HTTP_WithRouter(t *testing.T) {
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

	// Wire the transports via a shared StaticRouter.
	router := transport.NewStaticRouter()
	router.Register(1, "http://"+remoteA.ListenAddr())
	router.Register(2, "http://"+remoteB.ListenAddr())
	remoteA.SetRouter(router)
	remoteB.SetRouter(router)

	// App A with echo handler.
	a, err := New(
		WithNamespace("appa"),
		WithRuntimeSlot(1),
		WithRemoteTransport(remoteA),
		WithRootActor(func() actor.Actor { return &echoActor{} }),
	)
	if err != nil {
		t.Fatalf("New A: %v", err)
	}

	// App B with echo handler.
	b, err := New(
		WithNamespace("appb"),
		WithRuntimeSlot(2),
		WithRemoteTransport(remoteB),
		WithRootActor(func() actor.Actor { return &echoActor{} }),
	)
	if err != nil {
		t.Fatalf("New B: %v", err)
	}

	doneA := make(chan error, 1)
	doneB := make(chan error, 1)
	go func() { doneA <- a.Run(ctx) }()
	go func() { doneB <- b.Run(ctx) }()

	time.Sleep(100 * time.Millisecond)

	// App A calls App B's root actor over HTTP via RemoteRef + Router.
	refB := a.RemoteRef(b.Self().ID())
	call := refB.Invoke(ctx, "test.echo", []byte("hello-router"))
	val, err := call.Recv()
	if err != nil {
		t.Fatalf("A->B call error: %v", err)
	}
	if string(val.([]byte)) != "hello-router" {
		t.Fatalf("A->B result: got %q, want hello-router", val)
	}

	// App B calls App A's root actor over HTTP via RemoteRef + Router.
	refA := b.RemoteRef(a.Self().ID())
	call2 := refA.Invoke(ctx, "test.echo", []byte("world-router"))
	val2, err2 := call2.Recv()
	if err2 != nil {
		t.Fatalf("B->A call error: %v", err2)
	}
	if string(val2.([]byte)) != "world-router" {
		t.Fatalf("B->A result: got %q, want world-router", val2)
	}

	cancel()
	<-doneA
	<-doneB
}

// TestApp_CrossApp_Watch confirms that Watch and Terminated system messages
// are delivered across process boundaries via the remote transport.
func TestApp_CrossApp_Watch(t *testing.T) {
	ctxA, cancelA := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelA()
	ctxB, cancelB := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelB()

	remoteA, remoteB := transport.NewMemoryRemotePair()

	// App B: the target. Spawn a child actor that App A will watch.
	b, err := New(
		WithNamespace("appb"),
		WithRuntimeSlot(2),
		WithRemoteTransport(remoteB),
	)
	if err != nil {
		t.Fatalf("New B: %v", err)
	}

	doneB := make(chan error, 1)
	go func() { doneB <- b.Run(ctxB) }()

	// Wait for App B to start, then spawn a child actor.
	time.Sleep(50 * time.Millisecond)
	targetRef, err := b.Spawn(actor.PropsFromFunc(func() actor.Actor { return &actor.Host{} }), "target")
	if err != nil {
		t.Fatalf("spawn target: %v", err)
	}

	terminated := make(chan struct{}, 1)

	// App A: the watcher.
	a, err := New(
		WithNamespace("appa"),
		WithRuntimeSlot(1),
		WithRemoteTransport(remoteA),
		WithRootActor(func() actor.Actor {
			return &watchRootActor{
				targetRef:  targetRef,
				terminated: terminated,
			}
		}),
	)
	if err != nil {
		t.Fatalf("New A: %v", err)
	}

	doneA := make(chan error, 1)
	go func() { doneA <- a.Run(ctxA) }()

	time.Sleep(50 * time.Millisecond)

	// Stop the target actor in App B. Its OnStop notifies watchers.
	implB := b.(*appImpl)
	if err := implB.tree.Destroy(targetRef); err != nil {
		t.Fatalf("stop target: %v", err)
	}

	// Wait for App A's watcher to receive Terminated.
	select {
	case <-terminated:
		// Success.
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for cross-app Terminated")
	}

	cancelA()
	cancelB()
	<-doneA
	<-doneB
}

// TestApp_CrossApp_ServiceLookup confirms that App.LookupService resolves
// a remote service via a shared discovery provider and returns a Ref that
// can be invoked transparently across process boundaries.
func TestApp_CrossApp_ServiceLookup(t *testing.T) {
	ctxA, cancelA := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelA()
	ctxB, cancelB := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelB()

	remoteA, remoteB := transport.NewMemoryRemotePair()
	dp := discovery.NewMemoryProvider(time.Now())

	// App B: exposes a service via its root actor.
	b, err := New(
		WithNamespace("appb"),
		WithRuntimeSlot(2),
		WithRemoteTransport(remoteB),
		WithDiscoveryProvider(dp),
		WithRootActor(func() actor.Actor {
			return &serviceActor{callID: "test.echo", serviceName: "echo"}
		}),
	)
	if err != nil {
		t.Fatalf("New B: %v", err)
	}

	doneB := make(chan error, 1)
	go func() { doneB <- b.Run(ctxB) }()
	time.Sleep(50 * time.Millisecond)

	// App A: no local service, but shares the discovery provider.
	a, err := New(
		WithNamespace("appa"),
		WithRuntimeSlot(1),
		WithRemoteTransport(remoteA),
		WithDiscoveryProvider(dp),
	)
	if err != nil {
		t.Fatalf("New A: %v", err)
	}

	doneA := make(chan error, 1)
	go func() { doneA <- a.Run(ctxA) }()
	time.Sleep(50 * time.Millisecond)

	// App A looks up the service via discovery (service name is "echo",
	// the call ID is "test.echo").
	svcRef, ok := a.LookupService("echo")
	if !ok {
		t.Fatal("LookupService(echo): not found via discovery")
	}

	// Invoke the remote service transparently.
	call := svcRef.Invoke(ctxA, "test.echo", []byte("hello-discovery"))
	val, err := call.Recv()
	if err != nil {
		t.Fatalf("invoke via discovered ref: %v", err)
	}
	if string(val.([]byte)) != "hello-discovery" {
		t.Fatalf("result: got %q, want hello-discovery", val)
	}

	cancelA()
	cancelB()
	<-doneA
	<-doneB
}
