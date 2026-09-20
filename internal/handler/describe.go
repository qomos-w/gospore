package handler

import (
	"fmt"
	"reflect"

	tschema "github.com/qomos-w/spore/schema"
)

// errorType is the reflect.Type of the built-in error interface.
// Used to recognise the trailing-error return convention without
// pulling in spore's unexported describeFunctionReturns helper.
var errorType = reflect.TypeOf((*error)(nil)).Elem()

// DescribeHandler produces the spore CallableDesc for a gospore
// handler. It is the bridge from the user's Go function (which carries
// a leading actor.Context / actor.PureContext sentinel) to the
// schema-described callable shape (which carries only the wire-visible
// params + returns).
//
// Behaviour:
//
//  1. ClassifyHandlerMode is invoked first; any signature error short-
//     circuits with ErrInvalidHandlerSignature wrapping. The classifier
//     is the single source of truth for "is this even a handler?".
//  2. The first parameter (Context or PureContext) is stripped — it is
//     a runtime injection sentinel, not part of the wire signature.
//  3. Remaining parameters are described via spore's DescribeReflectType
//     and labelled "arg0", "arg1", ... so the manifest sees user-payload
//     positions (not the stripped Context).
//  4. Returns follow the unary convention: error must be last if present,
//     and at most one non-error return is allowed (the wire only carries
//     a single Reply value). Streaming descriptors (Streaming != nil)
//     are produced by a separate path (M09 item 8).
//
// spore's DescribeGoFunction cannot be reused directly because it
// calls DescribeReflectType on every input parameter — actor.Context
// is a method-bearing interface and would land in spore's unsupported
// branch. The strip-then-describe wrapping here is the gospore-side
// adapter for that constraint.
func DescribeHandler(callID string, fn any) (tschema.CallableDesc, error) {
	if _, err := ClassifyHandlerMode(fn); err != nil {
		return tschema.CallableDesc{}, err
	}
	typ := reflect.TypeOf(fn)

	desc := tschema.CallableDesc{
		Name:       callID,
		Parameters: make([]tschema.ParameterDesc, 0, typ.NumIn()-1),
		Mode:       tschema.CallableModeUnary,
	}
	hasEmitter := false
	for i := 1; i < typ.NumIn(); i++ {
		paramType := typ.In(i)
		if isEmitterType(paramType) {
			hasEmitter = true
			continue
		}
		td, err := tschema.DescribeReflectType(paramType)
		if err != nil {
			return tschema.CallableDesc{}, fmt.Errorf("%w: param %d (%s): %v",
				ErrInvalidHandlerSignature, i-1, paramType, err)
		}
		desc.Parameters = append(desc.Parameters, tschema.ParameterDesc{
			Name: fmt.Sprintf("arg%d", len(desc.Parameters)),
			Type: td,
		})
	}

	hasError, returns, err := describeReturns(typ)
	if err != nil {
		return tschema.CallableDesc{}, err
	}

	if hasEmitter {
		if len(returns) > 0 {
			return tschema.CallableDesc{}, fmt.Errorf("%w: streaming handler cannot have non-error returns",
				ErrInvalidHandlerSignature)
		}
		desc.Mode = tschema.CallableModeStreaming
		desc.HasError = hasError
		anyType := tschema.TypeDesc{Kind: tschema.TypeKindScalar, Name: "any"}
		desc.Streaming = &tschema.StreamingCallableDesc{
			Next: &anyType,
		}
		return desc, nil
	}

	desc.HasError = hasError
	desc.Returns = returns
	return desc, nil
}

// describeReturns walks fn's outputs under the unary convention and
// returns (hasError, returns, error). Mirrors the local logic of
// spore.describeFunctionReturns (which is unexported) so we can wrap
// failures in ErrInvalidHandlerSignature without dragging in spore's
// less specific error wrapping.
func describeReturns(typ reflect.Type) (bool, []tschema.TypeDesc, error) {
	n := typ.NumOut()
	if n == 0 {
		return false, nil, nil
	}
	hasError := false
	returns := make([]tschema.TypeDesc, 0, n)
	for i := 0; i < n; i++ {
		out := typ.Out(i)
		if out == errorType {
			if i != n-1 {
				return false, nil, fmt.Errorf("%w: error must be the last return value (found at position %d of %d)",
					ErrInvalidHandlerSignature, i, n)
			}
			hasError = true
			continue
		}
		if len(returns) > 0 {
			return false, nil, fmt.Errorf("%w: at most one non-error return allowed (got %s and %s)",
				ErrInvalidHandlerSignature, returns[0].Name, out)
		}
		td, err := tschema.DescribeReflectType(out)
		if err != nil {
			return false, nil, fmt.Errorf("%w: return %d (%s): %v",
				ErrInvalidHandlerSignature, i, out, err)
		}
		returns = append(returns, td)
	}
	return hasError, returns, nil
}
