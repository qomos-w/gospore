package gateway_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/gateway"
)

var (
	errReproQuota    = errors.New("repro-quota: all candidates 429")
	errReproNoTarget = errors.New("repro: no child service")
)

func toString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return ""
}

// Reproduction for: frontend invoke resolves undefined instead of the
// handler's error when the handler runs on a named loop and its planner
// child call fails. The client must receive an error frame.

type reproChildActor struct{ actor.Host }

func (a *reproChildActor) OnStart(ctx actor.Context) error {
	if err := ctx.RegisterDomain("childsvc").Expose(); err != nil {
		return err
	}
	return ctx.Register("child.fail", func(_ actor.Context, _ []byte) (string, error) {
		time.Sleep(100 * time.Millisecond)
		return "", errReproQuota
	})
}

type reproAgentActor struct{ actor.Host }

func (a *reproAgentActor) OnStart(ctx actor.Context) error {
	if err := ctx.RegisterLoop("agent_exec", actor.ModeStateful); err != nil {
		return err
	}
	if err := ctx.RegisterDomain("agent").Expose(); err != nil {
		return err
	}
	_, err := ctx.Spawn(actor.PropsFromFunc(func() actor.Actor { return &reproChildActor{} }), "child")
	if err != nil {
		return err
	}
	return ctx.Register("agent.top", func(c actor.Context, _ []byte) (string, error) {
		svcRef, ok := c.LookupService("childsvc")
		if !ok {
			return "", errReproNoTarget
		}
		node, err := c.Planner().Plan(svcRef, "child.fail", []byte("go"))
		if err != nil {
			return "", err
		}
		if err := node.Start(c.Lifecycle()); err != nil {
			return "", err
		}
		defer func() { _ = c.Stop(node.Ref()) }()

		timeoutCtx, cancel := context.WithTimeout(c.Lifecycle(), 2*time.Second)
		defer cancel()

		type result struct {
			val string
			err error
		}
		ch := make(chan result, 1)
		go func() {
			v, err := node.Recv()
			if err != nil {
				ch <- result{err: err}
				return
			}
			ch <- result{val: toString(v)}
		}()

		select {
		case r := <-ch:
			if r.err != nil {
				return "", r.err
			}
			return "unexpected-success:" + r.val, nil
		case <-timeoutCtx.Done():
			return "", errors.New("agent.top: handler timeout")
		}
	}, actor.WithLoop("agent_exec"))
}

type reproRootActor struct {
	actor.Host
}

func (a *reproRootActor) OnStart(ctx actor.Context) error {
	_, err := ctx.Spawn(actor.PropsFromFunc(func() actor.Actor { return &reproAgentActor{} }).WithPlanner(), "agent")
	return err
}

// A unary invoke whose handler is slower than the gateway timeout must
// surface as an ERROR frame to the client, never as a success reply with
// an empty payload (the original bug: RecvRaw returned io.EOF after
// Cancel, indistinguishable from a void-handler End).
func TestGatewayInvoke_GatewayTimeout_ReachesClientAsError(t *testing.T) {
	prev := gateway.GatewayInvokeTimeout
	gateway.GatewayInvokeTimeout = 80 * time.Millisecond
	defer func() { gateway.GatewayInvokeTimeout = prev }()

	_, addr, shutdown := newBinaryOnlyGatewayAppWithActor(t, gateway.Nop(), func() actor.Actor {
		return &reproTimeoutActor{}
	})
	defer shutdown()

	ws := mustDialWS(t, "ws://"+addr+"/ws")
	defer ws.Close()

	wire := &gateway.WireFrame{
		Flags:  gateway.MakeFlags(gateway.EncodingBinary, gateway.CompressionNone),
		Type:   gateway.FrameTypeInvoke,
		CorID:  1,
		CallID: "slow.svc.call",
	}
	mustWriteBinaryWire(t, ws, wire)

	reply := mustReadWireFrame(t, ws)
	if reply.Type == gateway.FrameTypeReply {
		t.Fatalf("BUG: gateway timeout masked as success reply (payload=%q)", string(reply.Payload))
	}
	if reply.Type != gateway.FrameTypeError {
		t.Fatalf("expected error frame, got %v", reply.Type)
	}
	if !strings.Contains(reply.ErrorMsg, "gateway timeout") {
		t.Fatalf("error message = %q, want gateway timeout", reply.ErrorMsg)
	}
}

type reproTimeoutActor struct{ actor.Host }

func (a *reproTimeoutActor) OnStart(ctx actor.Context) error {
	if err := ctx.RegisterDomain("slow").Expose(); err != nil {
		return err
	}
	return ctx.Register("slow.svc.call", func(_ actor.Context, _ []byte) (string, error) {
		time.Sleep(2 * time.Second)
		return "too-late", nil
	})
}

// A loop-scheduled handler whose planner child call fails must surface the
// error to the client (pins the loop + planner child-error path).
func TestGatewayInvoke_LoopHandlerChildError_ReachesClientAsError(t *testing.T) {
	_, addr, shutdown := newBinaryOnlyGatewayAppWithActor(t, gateway.Nop(), func() actor.Actor {
		return &reproRootActor{}
	})
	defer shutdown()

	ws := mustDialWS(t, "ws://"+addr+"/ws")
	defer ws.Close()

	wire := &gateway.WireFrame{
		Flags:  gateway.MakeFlags(gateway.EncodingBinary, gateway.CompressionNone),
		Type:   gateway.FrameTypeInvoke,
		CorID:  1,
		CallID: "agent.top",
	}
	mustWriteBinaryWire(t, ws, wire)

	reply := mustReadWireFrame(t, ws)
	if reply.Type == gateway.FrameTypeReply {
		t.Fatalf("BUG REPRODUCED: handler error masked as success reply (payload=%q)", string(reply.Payload))
	}
	if reply.Type != gateway.FrameTypeError {
		t.Fatalf("expected error frame, got %v", reply.Type)
	}
	if !strings.Contains(reply.ErrorMsg, "repro-quota") {
		t.Fatalf("error message = %q, want repro-quota", reply.ErrorMsg)
	}
}
