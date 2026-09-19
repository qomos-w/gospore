package handler

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/qomos-w/gospore/actor"
)

// ErrInvalidHandlerSignature is returned by ClassifyHandlerMode when the
// supplied value does not satisfy the structural prerequisites of a
// gospore handler: must be a non-nil function, must not be variadic,
// and must take at least one parameter (the context).
//
// Callers that wrap classifier errors at the Register entry point map
// this sentinel to the canonical wire diag (gospore.callable.signature_mismatch).
// The classifier itself stays diag-code-agnostic so it can be used for
// both Go registration and script-bridge introspection.
var ErrInvalidHandlerSignature = errors.New("gospore/internal/handler: invalid handler signature")

// contextType / pureContextType are the reflect.Type values for
// actor.Context and actor.PureContext. They are computed once at init
// to avoid the reflect.TypeOf((*actor.Context)(nil)).Elem() spelling at
// every classifier call.
var (
	contextType     = reflect.TypeOf((*actor.Context)(nil)).Elem()
	pureContextType = reflect.TypeOf((*actor.PureContext)(nil)).Elem()
	emitterType     = reflect.TypeOf((*actor.Emitter)(nil)).Elem()
)

// isEmitterType reports whether t is actor.Emitter or actor.EmitterT[R].
// It uses duck-typing (Send + Done methods) rather than name matching so
// it survives Go version differences in how reflect.Type.Name() formats
// instantiated generic interfaces.
func isEmitterType(t reflect.Type) bool {
	if t == emitterType {
		return true
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Interface {
		return false
	}
	hasSend, hasDone := false, false
	for i := 0; i < t.NumMethod(); i++ {
		switch t.Method(i).Name {
		case "Send":
			hasSend = true
		case "Done":
			hasDone = true
		}
	}
	return hasSend && hasDone
}

// emitterSendMethod returns the "Send" method descriptor on an emitter
// interface type, used to reflect the streaming chunk type out of
// actor.Emitter / actor.EmitterT[R]. Returns nil if t exposes no Send
// method (caller is expected to have already passed isEmitterType).
func emitterSendMethod(t reflect.Type) *reflect.Method {
	for i := 0; i < t.NumMethod(); i++ {
		m := t.Method(i)
		if m.Name == "Send" {
			return &m
		}
	}
	return nil
}

// ClassifyHandlerMode inspects fn's reflect.Type and returns the
// HandlerMode implied by its first parameter. The rule (per
// ARCHITECTURE.md §4.12) is exact-match:
//
//   - first param == actor.Context     → ModeStateful
//   - first param == actor.PureContext → ModeStateless
//
// Any other shape — nil, non-func, variadic, zero-param, first-param
// other than Context/PureContext — is rejected with an error that
// wraps ErrInvalidHandlerSignature so callers can errors.Is against it.
//
// On error the returned HandlerMode is unspecified; callers must check
// err before consulting the mode (this is the standard Go convention,
// but worth flagging because actor.HandlerMode's zero value aliases
// ModeStateful and a missed err check would silently look stateful).
//
// The match is deliberately exact (reflect.Type ==) rather than
// structural (Type.Implements). Users declare `func(actor.Context, ...)`
// or `func(actor.PureContext, ...)`; passing a concrete type that
// happens to satisfy Context is not how the surface is meant to be
// driven, and exact-match keeps the inferred mode unambiguous when
// Context's method set strictly supersets PureContext's.
func ClassifyHandlerMode(fn any) (actor.HandlerMode, error) {
	if fn == nil {
		return 0, fmt.Errorf("%w: handler is nil", ErrInvalidHandlerSignature)
	}
	typ := reflect.TypeOf(fn)
	if typ.Kind() != reflect.Func {
		return 0, fmt.Errorf("%w: expected func, got %s", ErrInvalidHandlerSignature, typ.Kind())
	}
	if typ.IsVariadic() {
		return 0, fmt.Errorf("%w: variadic handlers are not supported", ErrInvalidHandlerSignature)
	}
	if typ.NumIn() == 0 {
		return 0, fmt.Errorf("%w: handler must accept at least one parameter (Context or PureContext)", ErrInvalidHandlerSignature)
	}
	first := typ.In(0)
	switch first {
	case contextType:
		return actor.ModeStateful, nil
	case pureContextType:
		return actor.ModeStateless, nil
	default:
		return 0, fmt.Errorf("%w: first parameter must be actor.Context or actor.PureContext, got %s", ErrInvalidHandlerSignature, first)
	}
}
