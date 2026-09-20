package app

// Invoke plumbing: refs, routing, pending tables, and schema lookups.
import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/message"

	"github.com/qomos-w/gospore/codec"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/ref"
	"github.com/qomos-w/gospore/schema"
	tschema "github.com/qomos-w/spore/schema"
)

// App is the process-level entry point and root actor of one
// gospore service. App embeds actor.Actor: it has its own OnStart /
// OnStop chain, runs handlers in the `app.*` reserved namespace, and
type remoteRef struct {
	aid    id.ActorID
	invoke func(ctx context.Context, callID string, payload any, headers ...map[string]string) *invoke.Call
}

// cancellingStream wraps an invoke.Stream and sends a KindCancel frame
// to the target actor when Cancel() is called. Normal Close() does not
// send cancellation — only the explicit Cancel() path does.
type cancellingStream struct {
	invoke.Stream
	sendCancel func() error
	once       sync.Once
}

func (s *cancellingStream) Cancel() error {
	s.once.Do(func() {
		_ = s.sendCancel()
	})
	if cs, ok := s.Stream.(interface{ Cancel() error }); ok {
		return cs.Cancel()
	}
	return s.Stream.Close()
}

func (r *remoteRef) ID() id.ActorID          { return r.aid }
func (r *remoteRef) Service() (string, bool) { return "", false }
func (r *remoteRef) Invoke(ctx context.Context, callID string, payload any, headers ...map[string]string) *invoke.Call {
	if r.invoke != nil {
		return r.invoke(ctx, callID, payload, headers...)
	}
	return nil
}

var _ ref.Ref = (*remoteRef)(nil)

// RemoteRef returns a Ref for a remote actor. Invoke calls on the
// returned Ref route through the App's remote transport with the
// App's root actor as the caller.
func (a *appImpl) RemoteRef(actorID id.ActorID) ref.Ref {
	return &remoteRef{
		aid: actorID,
		invoke: func(ctx context.Context, callID string, payload any, headers ...map[string]string) *invoke.Call {
			mode := a.lookupCallMode(actorID, callID)
			return invoke.NewCall(mode, a.invoke(a.rootRef.ID(), actorID, callID, payload, headers...), invoke.WithFinalTimeout(a.cfg.defaultInvokeTimeout))
		},
	}
}

// invokeAs issues a call on behalf of caller with caller as the message
// sender, so ctx.Caller() in the target's handlers resolves to the calling
// actor instead of the target itself (which is what a bare target.Invoke
// produces). Local targets carry the caller's role header; remote targets
// keep the legacy sender semantics (app root) because remote reply routing
// addresses the App, not an individual local actor.
func (a *appImpl) invokeAs(caller, target ref.Ref, callID string, payload any, headers ...map[string]string) *invoke.Call {
	targetID := target.ID()
	if _, ok := a.tree.LookupID(targetID); !ok {
		return target.Invoke(context.Background(), callID, payload, headers...)
	}
	mode := a.lookupCallMode(targetID, callID)
	headers = a.injectCallerRole(caller.ID(), headers)
	return invoke.NewCall(mode, a.invoke(caller.ID(), targetID, callID, payload, headers...), invoke.WithFinalTimeout(a.cfg.defaultInvokeTimeout))
}

// SpawnGatewaySession spawns an empty Host cell under root. The per-cell
// pending table is reserved by the tree allocator's Reserve phase
// (reserveSpawn), so calls issued through the returned ref are isolated
// from the moment Spawn returns.
func (a *appImpl) InvokeAsCaller(caller, target ref.Ref, callID string, payload any, headers ...map[string]string) *invoke.Call {
	return a.invokeAs(caller, target, callID, payload, headers...)
}

// DestroyGatewaySession removes the session cell from the tree. The
// terminate path delivers Destroy to the cell, whose handleDestroy
// flushes the pending table (terminal errors to every in-flight call)
// before the registry entry is dropped.
func (a *appImpl) injectCallerRole(caller id.ActorID, headers []map[string]string) []map[string]string {
	for _, h := range headers {
		if h != nil {
			if _, ok := h["gospore.caller_role"]; ok {
				return headers
			}
		}
	}
	if c := a.getCell(caller); c != nil {
		if role := c.Role(); role != "" {
			return append(headers, map[string]string{"gospore.caller_role": role})
		}
	}
	return headers
}

// invokeTableFor returns the pending table that correlates replies for calls
// issued by caller. Every spawned actor owns one (reserved at spawn); the
// root entry aliases the App table, which also homes gateway/App-originated
// calls. Unknown callers fall back to the App table so a call can never
// register into a table whose reply routing cannot reach it.
func (a *appImpl) invokeTableFor(caller id.ActorID) *invoke.PendingTable {
	a.invokeTableMu.RLock()
	pt, ok := a.invokeTables[caller]
	a.invokeTableMu.RUnlock()
	if ok {
		return pt
	}
	return a.pending
}

func (a *appImpl) setInvokeTable(aid id.ActorID, pt *invoke.PendingTable) {
	a.invokeTableMu.Lock()
	a.invokeTables[aid] = pt
	a.invokeTableMu.Unlock()
}

func (a *appImpl) removeInvokeTable(aid id.ActorID) {
	if aid == a.rootRef.ID() {
		return
	}
	a.invokeTableMu.Lock()
	delete(a.invokeTables, aid)
	a.invokeTableMu.Unlock()
}

// invoke sends a Call frame to target and returns a Stream backed by
// the pending table. Caller and Sender are resolved through the Tree.
//
// Encode path: []byte payloads pass through unchanged; non-[]byte
// payloads are encoded via the App's codec using the target callable's
// parameter TypeDesc. If the descriptor is not yet available (target
// actor has not finished OnStart) the body falls back to nil.
// Optional headers maps are merged into Frame.Headers left-to-right.
func (a *appImpl) invoke(caller, target id.ActorID, callID string, payload any, headers ...map[string]string) invoke.Stream {
	// Reply frames are addressed To: caller and land in the caller's own
	// reply pipeline, so the slot must live in the caller's pending table.
	pt := a.invokeTableFor(caller)
	var body []byte
	payloadMode := message.PayloadModeValue
	encoding := message.EncodingNone
	var schemaID uint64
	encoded := false
	if b, ok := payload.([]byte); ok {
		desc, reqSchemaID, descFound := a.lookupCallableMeta(target, callID)
		if descFound && len(desc.Parameters) > 0 && codec.IsTBCData(b) {
			// Binary (TBC) payload — pass through as Raw with Binary encoding so
			// the receiving cell can decode it via the multi-codec dispatch path.
			body = b
			payloadMode = message.PayloadModeRaw
			encoding = message.EncodingBinary
			schemaID = reqSchemaID
			encoded = true
		} else {
			body = b
			schemaID = schema.BuiltinBytes
		}
	} else if s, ok := payload.(string); ok {
		body = []byte(s)
		schemaID = schema.BuiltinString
	} else if payload != nil && a.codec != nil {
		desc, requestSchemaID, descFound := a.lookupCallableMeta(target, callID)
		if descFound && len(desc.Parameters) > 0 {
			var err error
			body, err = a.codec.Encode(desc.Parameters[0].Type, payload)
			if err != nil {
				body = nil
			} else {
				encoded = true
				payloadMode = message.PayloadModeRaw
				encoding = a.codec.Encoding()
				schemaID = requestSchemaID
			}
		}
	}

	corID := uint64(a.corIDGen.Next())
	ch := make(chan message.Frame, 16)
	pt.Register(corID, callID, string(a.lookupCallMode(target, callID)), ch)

	callerRef, _ := a.tree.LookupID(caller)
	targetRef, _ := a.tree.LookupID(target)

	hdr := make(map[string]string)
	for _, h := range headers {
		for k, v := range h {
			hdr[k] = v
		}
	}

	envPayload := payload
	if encoded {
		// Body is canonical when the codec successfully encoded a non-[]byte
		// payload — clearing envPayload forces the receiving cell to decode
		// from Body via buildArgs instead of trying to assign the raw Go
		// value (e.g. map[string]any from a JSON gateway) directly to a
		// typed handler param.
		envPayload = nil
	}

	env := mailbox.Envelope{
		Frame: message.Frame{
			From:        caller,
			To:          target,
			Kind:        message.KindCall,
			CorID:       corID,
			CallID:      callID,
			SchemaID:    schemaID,
			PayloadMode: payloadMode,
			Encoding:    encoding,
			Body:        body,
			Headers:     hdr,
		},
		Sender:  callerRef,
		Payload: envPayload,
	}

	if targetRef != nil {
		if err := a.deliver(targetRef, env); err != nil {
			pt.SendFailed(corID)
			return invoke.NewErrorStream(err)
		}
	} else if a.remoteTP != nil {
		if err := a.remoteTP.Send(env.Frame); err != nil {
			pt.SendFailed(corID)
			return invoke.NewErrorStream(err)
		}
	} else {
		pt.SendFailed(corID)
		return invoke.NewErrorStream(fmt.Errorf("target not found"))
	}

	stream := invoke.NewStreamWithTypedDecode(ch, corID, pt, a.codec, a.lookupTypedDecode(target, callID))
	wrapped := &cancellingStream{
		Stream: stream,
		sendCancel: func() error {
			cancelFrame := message.Frame{
				From:   caller,
				To:     target,
				Kind:   message.KindCancel,
				CorID:  corID,
				CallID: callID,
			}
			if targetRef != nil {
				return a.deliver(targetRef, mailbox.Envelope{Frame: cancelFrame})
			} else if a.remoteTP != nil {
				return a.remoteTP.Send(cancelFrame)
			}
			return nil
		},
	}
	return wrapped
}

// lookupCallableMeta resolves the registered descriptor and request schema ID
// for callID on the target actor. Returns (zero, 0, false) when unavailable.
func (a *appImpl) lookupCallableMeta(target id.ActorID, callID string) (tschema.CallableDesc, uint64, bool) {
	targetCell := a.getCell(target)
	if targetCell == nil {
		return tschema.CallableDesc{}, 0, false
	}
	if ht := targetCell.Handlers(); ht != nil {
		desc, ok := ht.Desc(callID)
		if !ok {
			return tschema.CallableDesc{}, 0, false
		}
		inv, _ := ht.Lookup(callID)
		if inv == nil {
			return desc, 0, true
		}
		return desc, inv.RequestSchemaID, true
	}
	return tschema.CallableDesc{}, 0, false
}

// lookupCallableDesc resolves the schema descriptor for callID on the
// target actor. Returns (zero, false) when the cell or descriptor is
// not found — callers must handle the fallback themselves.
func (a *appImpl) lookupCallableDesc(target id.ActorID, callID string) (tschema.CallableDesc, bool) {
	desc, _, ok := a.lookupCallableMeta(target, callID)
	return desc, ok
}

// lookupTypedDecode returns a hook that decodes raw replies for callID on
// target into the concrete Go type derived from the handler (streaming
// ChunkType or the unary first return value). Returns nil when the type
// cannot be determined — Recv then decodes via the generic codec path.
//
// The reply type is discovered at runtime via reflection (lookupReplyType),
// so the hook allocates its decode target with reflect.New per frame;
// callers that know the reply type at compile time should build their
// stream with invoke.NewStreamAs, which decodes through codec.DecodeAs
// without any reflection.
func (a *appImpl) lookupTypedDecode(target id.ActorID, callID string) invoke.TypedDecode {
	replyType := a.lookupReplyType(target, callID)
	if replyType == nil {
		return nil
	}
	return func(schemaID uint64, encoding message.Encoding, body []byte) (any, bool) {
		out := reflect.New(replyType)
		if err := a.codec.DecodeByIDInto(schemaID, encoding, body, out.Interface()); err != nil {
			return nil, false
		}
		return out.Elem().Interface(), true
	}
}

// lookupReplyType returns the concrete Go type the caller should expect
// from reply frames for callID on target. For streaming callables this is
// ChunkType; for unary callables it derives from the handler's first return
// value. Returns nil when the type cannot be determined (remote target,
// script-backed handler, or built-in scalar).
func (a *appImpl) lookupReplyType(target id.ActorID, callID string) reflect.Type {
	targetCell := a.getCell(target)
	if targetCell == nil {
		return nil
	}
	ht := targetCell.Handlers()
	if ht == nil {
		return nil
	}
	inv, ok := ht.Lookup(callID)
	if !ok || inv == nil {
		return nil
	}
	// Streaming: the chunk type is definitive.
	if inv.ChunkType != nil {
		return inv.ChunkType
	}
	// Unary: derive from the handler function's first return value.
	if inv.Fn != nil {
		fnType := reflect.TypeOf(inv.Fn)
		if fnType.Kind() == reflect.Func && fnType.NumOut() > 0 {
			out := fnType.Out(0)
			// If the first return is error and there are 2 returns, the
			// value return is actually the second one (unusual but possible).
			if fnType.NumOut() == 2 && out == errorType {
				out = fnType.Out(1)
			}
			if out.Kind() == reflect.Struct {
				return out
			}
			if out.Kind() == reflect.Pointer && out.Elem().Kind() == reflect.Struct {
				return out.Elem()
			}
		}
	}
	return nil
}

// lookupCallMode resolves the CallMode for a target callable based on
// its registered descriptor. Falls back to CallModeUnary when the
// descriptor is not yet available.
func (a *appImpl) lookupCallMode(target id.ActorID, callID string) invoke.CallMode {
	desc, ok := a.lookupCallableDesc(target, callID)
	if !ok {
		return invoke.CallModeUnary
	}
	switch desc.Mode {
	case tschema.CallableModeStreaming:
		return invoke.CallModeStream
	default:
		// Tell: no returns and no error → fire-and-forget.
		if len(desc.Returns) == 0 && !desc.HasError {
			return invoke.CallModeTell
		}
		return invoke.CallModeUnary
	}
}

// appRef is the concrete ref.Ref implementation produced by App.
type appRef struct {
	aid    id.ActorID
	invoke func(ctx context.Context, callID string, payload any, headers ...map[string]string) *invoke.Call
}

func (r *appRef) ID() id.ActorID          { return r.aid }
func (r *appRef) Service() (string, bool) { return "", false }
func (r *appRef) Invoke(ctx context.Context, callID string, payload any, headers ...map[string]string) *invoke.Call {
	if r.invoke != nil {
		return r.invoke(ctx, callID, payload, headers...)
	}
	return nil
}

var _ ref.Ref = (*appRef)(nil)
