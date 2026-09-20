package handler

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/codec"
	"github.com/qomos-w/gospore/message"
	tschema "github.com/qomos-w/spore/schema"
)

// buildRunClosure constructs the Invoker.Run closure for a registered
// handler. The closure captures the handler function, its reflect.Type,
// and the return value's TypeDesc so each invocation can:
//
//  1. Build reflect args: first param is a zero-value Context/PureContext
//     (actual context injection is Cell-tier work), Emitter params get an
//     emitterImpl, remaining params are populated from env.Payload.
//  2. Call the handler via reflect.ValueOf(fn).Call(args).
//  3. Encode the return value via env.Codec + returnDesc and emit the
//     Reply/End/Error frame sequence via env.Reply.
//
// Return value convention (ARCHITECTURE.md §4.12):
//   - 0 returns: send End only
//   - 1 return (error): send Error frame if non-nil, then End
//   - 1 return (T, no error): send Reply{T}, then End
//   - 2 returns (T, error): send Error frame if err != nil, else Reply{T}, then End
// resolveReplyEncoding picks the encoding to use for replies. The inbound
// frame's Encoding is honored when present; otherwise a gateway-set header
// can request binary replies for binary-only WebSocket connections.
func resolveReplyEncoding(env InvokeEnv) message.Encoding {
	enc := env.Frame.Encoding
	if enc == message.EncodingNone {
		if env.Frame.Headers != nil && env.Frame.Headers["gospore.reply_encoding"] == "binary" {
			return message.EncodingBinary
		}
	}
	return enc
}

func buildRunClosure(fn any, mode actor.HandlerMode, desc tschema.CallableDesc, returnSchemaID, chunkSchemaID uint64, chunkTypeDesc tschema.TypeDesc) func(InvokeEnv) {
	fv := reflect.ValueOf(fn)
	fnType := fv.Type()

	var returnDesc tschema.TypeDesc
	if len(desc.Returns) > 0 {
		returnDesc = desc.Returns[0]
	}
	paramDescs := desc.Parameters

	_ = paramDescs

	return func(env InvokeEnv) {
		replyEncoding := resolveReplyEncoding(env)
		args, decodeErr := buildArgs(fnType, mode, env, returnDesc, chunkSchemaID, chunkTypeDesc, replyEncoding)
		if decodeErr != nil {
			env.Reply(message.Frame{
				Kind:  message.KindError,
				CorID: env.Frame.CorID,
				Body:  []byte(fmt.Sprintf("gospore.handler.decode: %s", decodeErr.Error())),
			})
			return
		}
		var em *emitterImpl
		for i := 1; i < fnType.NumIn(); i++ {
			if isEmitterType(fnType.In(i)) {
				if e, ok := args[i].Interface().(*emitterImpl); ok {
					em = e
				}
				break
			}
		}
		if em != nil && env.OnCancel != nil {
			env.OnCancel(env.Frame.CorID, em.closeDoneCh)
		}
		if em != nil {
			// Streaming handler: run in a goroutine so it does not block the
			// actor loop. The loop can continue processing other messages while
			// the stream pumps chunks via emit.Send().
			go func() {
				defer func() {
					if r := recover(); r != nil {
						env.Reply(message.Frame{
							Kind:  message.KindError,
							CorID: env.Frame.CorID,
							Body:  []byte(fmt.Sprintf("actor panic: %v", r)),
						})
						env.Reply(message.Frame{Kind: message.KindEnd, CorID: env.Frame.CorID})
					}
					if env.OffCancel != nil {
						env.OffCancel(env.Frame.CorID)
					}
					em.closeDoneCh()
				}()
				results := fv.Call(args)
				emitResults(results, fnType, env, returnDesc, returnSchemaID, replyEncoding)
			}()
			return
		}
		defer func() {
			if env.OffCancel != nil {
				env.OffCancel(env.Frame.CorID)
			}
		}()
		results := fv.Call(args)
		emitResults(results, fnType, env, returnDesc, returnSchemaID, replyEncoding)
	}
}

// buildArgs constructs the reflect.Value slice for the handler call.
//   - First arg: injected Context or PureContext from env.Context.
//   - Emitter params: injected emitterImpl wrapping env.Reply.
//   - Remaining args: populated from env.Payload / frame metadata.
func buildArgs(fnType reflect.Type, mode actor.HandlerMode, env InvokeEnv, returnDesc tschema.TypeDesc, chunkSchemaID uint64, chunkTypeDesc tschema.TypeDesc, replyEncoding message.Encoding) ([]reflect.Value, error) {
	numIn := fnType.NumIn()
	args := make([]reflect.Value, numIn)

	ctxType := fnType.In(0)
	if env.Context != nil {
		cv := reflect.ValueOf(env.Context)
		if cv.Type().AssignableTo(ctxType) {
			args[0] = cv
		} else {
			args[0] = reflect.Zero(ctxType)
		}
	} else {
		args[0] = reflect.Zero(ctxType)
	}

	for i := 1; i < numIn; i++ {
		paramType := fnType.In(i)
		if isEmitterType(paramType) {
			var em actor.Emitter = &emitterImpl{
				reply:         env.Reply,
				corID:         env.Frame.CorID,
				done:          make(chan struct{}),
				codec:         env.Codec,
				returnDesc:    returnDesc,
				chunkSchemaID: chunkSchemaID,
				chunkTypeDesc: chunkTypeDesc,
				replyEncoding: replyEncoding,
			}
			args[i] = reflect.ValueOf(em)
			continue
		}

		payload := env.Payload
		if env.Frame.PayloadMode == message.PayloadModeRaw {
			if env.Codec != nil && len(env.Frame.Body) > 0 {
				dst := reflect.New(paramType)
				if err := env.Codec.DecodeByIDInto(env.Frame.SchemaID, env.Frame.Encoding, env.Frame.Body, dst.Interface()); err != nil {
					return nil, fmt.Errorf("binary decode %s: %w", paramType.Name(), err)
				}
				args[i] = dst.Elem()
			} else {
				args[i] = reflect.Zero(paramType)
			}
		} else if payload == nil {
			switch {
			case paramType.Kind() == reflect.String:
				args[i] = reflect.ValueOf(string(env.Frame.Body))
			case paramType.Kind() == reflect.Slice && paramType.Elem().Kind() == reflect.Uint8:
				args[i] = reflect.ValueOf(env.Frame.Body)
			default:
				args[i] = reflect.Zero(paramType)
			}
		} else {
			pv := reflect.ValueOf(payload)
			if pv.Type().AssignableTo(paramType) {
				args[i] = pv
			} else if body, ok := payload.([]byte); ok && len(body) > 0 && paramType.Kind() == reflect.Struct {
				// Inter-actor remote transport sends JSON-encoded structs
				// as []byte with PayloadModeValue. Decode via JSON.
				dst := reflect.New(paramType)
				if err := json.Unmarshal(body, dst.Interface()); err != nil {
					return nil, fmt.Errorf("json decode %s: %w", paramType.Name(), err)
				}
				args[i] = dst.Elem()
			} else {
				args[i] = reflect.Zero(paramType)
			}
		}
	}

	return args, nil
}


// emitResults processes the handler's return values and emits the
// appropriate frame sequence via env.Reply.
func emitResults(results []reflect.Value, fnType reflect.Type, env InvokeEnv, returnDesc tschema.TypeDesc, returnSchemaID uint64, replyEncoding message.Encoding) {
	corID := env.Frame.CorID

	switch len(results) {
	case 0:
		env.Reply(message.Frame{Kind: message.KindEnd, CorID: corID})

	case 1:
		out := results[0]
		if fnType.Out(0) == errorType {
			if err, _ := out.Interface().(error); err != nil {
				env.Reply(message.Frame{
					Kind:  message.KindError,
					CorID: corID,
					Body:  []byte(fmt.Sprintf("gospore.handler.error: %s", err.Error())),
				})
			}
			env.Reply(message.Frame{Kind: message.KindEnd, CorID: corID})
		} else {
			emitValueReply(env, corID, out, returnDesc, returnSchemaID, replyEncoding)
		}

	case 2:
		valOut, errOut := results[0], results[1]
		if err, _ := errOut.Interface().(error); err != nil {
			env.Reply(message.Frame{
				Kind:  message.KindError,
				CorID: corID,
				Body:  []byte(fmt.Sprintf("gospore.handler.error: %s", err.Error())),
			})
			env.Reply(message.Frame{Kind: message.KindEnd, CorID: corID})
		} else {
			emitValueReply(env, corID, valOut, returnDesc, returnSchemaID, replyEncoding)
		}
	}
}

// emitValueReply encodes v through env.Codec and emits the Reply+End pair.
// Encoding errors are surfaced as an Error frame in place of the Reply so
// the caller observes an explicit failure rather than a truncated stream.
func emitValueReply(env InvokeEnv, corID uint64, v reflect.Value, desc tschema.TypeDesc, schemaID uint64, replyEncoding message.Encoding) {
	body, mode, encoding, err := encodeReflectValue(v, env.Codec, desc, replyEncoding)
	if err != nil {
		env.Reply(message.Frame{
			Kind:  message.KindError,
			CorID: corID,
			Body:  []byte(fmt.Sprintf("gospore.handler.codec_encode: %s", err.Error())),
		})
		env.Reply(message.Frame{Kind: message.KindEnd, CorID: corID})
		return
	}
	env.Reply(message.Frame{
		Kind:        message.KindReply,
		CorID:       corID,
		SchemaID:    schemaID,
		PayloadMode: mode,
		Encoding:    encoding,
		Body:        body,
	})
	env.Reply(message.Frame{Kind: message.KindEnd, CorID: corID})
}


// emitterImpl implements actor.Emitter for the Run closure. Each Send
// call emits a Reply frame with incrementing Seq via the Reply callback.
// Done returns a channel that is closed when the handler finishes or the
// stream is cancelled.
type emitterImpl struct {
	reply         func(message.Frame)
	corID         uint64
	seq           uint32
	done          chan struct{}
	codec         codec.Codec
	returnDesc    tschema.TypeDesc
	chunkSchemaID uint64
	chunkTypeDesc tschema.TypeDesc
	replyEncoding message.Encoding
	closeDone     sync.Once
}

func (e *emitterImpl) closeDoneCh() {
	e.closeDone.Do(func() { close(e.done) })
}

func (e *emitterImpl) Send(chunk any) error {
	// Use the declared chunk type description when available (set via
	// Streaming[T]()); fall back to returnDesc for legacy handlers that
	// do not declare a concrete chunk type. When both are empty (e.g.
	// Streaming[any] event handlers), derive from the actual value so
	// the binary codec has a schema to encode against.
	desc := e.chunkTypeDesc
	if desc.Kind == "" {
		desc = e.returnDesc
	}
	if desc.Kind == "" || desc.Name == "any" {
		desc, _ = tschema.DescribeReflectType(reflect.TypeOf(chunk))
	}
	body, mode, encoding, err := encodeReflectValue(reflect.ValueOf(chunk), e.codec, desc, e.replyEncoding)
	if err != nil {
		return fmt.Errorf("gospore.emitter.codec_encode: %w", err)
	}
	seq := e.seq
	e.seq++
	e.reply(message.Frame{
		Kind:        message.KindReply,
		CorID:       e.corID,
		Seq:         seq,
		SchemaID:    e.chunkSchemaID,
		PayloadMode: mode,
		Encoding:    encoding,
		Body:        body,
	})
	return nil
}

func (e *emitterImpl) Done() <-chan struct{} {
	return e.done
}

func encodeReflectValue(v reflect.Value, c codec.Codec, desc tschema.TypeDesc, replyEncoding message.Encoding) ([]byte, message.PayloadMode, message.Encoding, error) {
	if !v.IsValid() {
		return nil, message.PayloadModeValue, message.EncodingNone, nil
	}
	iface := v.Interface()
	// []byte keeps the raw Value-mode passthrough (Body IS the value, and the
	// decode side returns Value-mode bodies unchanged). Every other type —
	// strings included — must round-trip through the codec with its schema ID
	// (BuiltinString for string) so callers decode a typed value; a raw
	// Value-mode string would arrive as []byte on the caller side.
	if b, ok := iface.([]byte); ok {
		return b, message.PayloadModeValue, message.EncodingNone, nil
	}
	if c == nil {
		return nil, 0, 0, fmt.Errorf("no codec configured for %T (App lacks WithCodec and no default codec is wired)", iface)
	}
	if _, ok := iface.(string); ok && desc.Kind == "" {
		desc = tschema.TypeDesc{Kind: tschema.TypeKindScalar, Name: "string"}
	}
	encoding := c.Encoding()
	body, err := c.Encode(desc, iface)
	if err != nil {
		return nil, 0, 0, err
	}
	// If the caller requested a specific reply encoding different from the
	// codec's default, try to re-encode via EncodingAwareCodec.
	if replyEncoding != message.EncodingNone && replyEncoding != encoding {
		if ec, ok := c.(codec.EncodingAwareCodec); ok {
			reEncoded, reErr := ec.EncodeAs(desc, iface, replyEncoding)
			if reErr == nil {
				body = reEncoded
				encoding = replyEncoding
			} else if replyEncoding == message.EncodingBinary {
				return nil, 0, 0, fmt.Errorf("binary encode %T: %w", iface, reErr)
			}
		}
	}
	return body, message.PayloadModeRaw, encoding, nil
}
