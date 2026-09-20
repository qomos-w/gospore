package handler

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/qomos-w/gospore/actor"
	tschema "github.com/qomos-w/spore/schema"
)

// ErrSignatureMismatch is the sentinel returned by DescConsistent when
// two CallableDesc values are not structurally equivalent. The Register
// entry point wraps this into the canonical wire diag
// gospore.callable.signature_mismatch (see actor.DiagCallableSignatureMismatch).
//
// Kept separate from ErrInvalidHandlerSignature: that one means "this
// single handler's reflective shape is malformed"; this one means "two
// otherwise-valid descriptors disagree on a callID's contract".
var ErrSignatureMismatch = errors.New("gospore/internal/handler: callable signature mismatch")

// ErrModeMismatch is the sentinel returned by ModeConsistent when two
// HandlerMode values disagree. Maps to gospore.callable.mode_mismatch
// at Register entry (actor.DiagCallableModeMismatch).
var ErrModeMismatch = errors.New("gospore/internal/handler: handler mode mismatch")

// ErrVisibilityMismatch is the sentinel returned by VisibilityConsistent
// when two Visibility values disagree. Maps to gospore.callable.visibility_mismatch
// at Register entry (actor.DiagCallableVisibilityMismatch).
var ErrVisibilityMismatch = errors.New("gospore/internal/handler: visibility mismatch")

// ErrLoopMismatch is the sentinel returned by LoopConsistent
// when two logical loop routes disagree.
var ErrLoopMismatch = errors.New("gospore/internal/handler: handler loop mismatch")

// DescConsistent reports whether the incoming CallableDesc is structurally
// equivalent to an existing one. Returns nil on match; otherwise an error
// wrapping ErrSignatureMismatch with a human-readable cause for diagnostics.
//
// Equivalence rule (ARCHITECTURE.md §4.12 step 5 + §4.6):
//
//   - Name (callID) must match — passing two descs with different Names
//     is a misuse but caught explicitly so the diagnostic message is clear.
//   - Mode (Unary / Streaming) must match.
//   - Streaming descriptor compared via reflect.DeepEqual; covers Next /
//     Final / Message / nested message-streaming Start / Delta / End.
//   - Parameters: same arity; each parameter's Type compared via DeepEqual
//     in order. Parameter NAMES are deliberately ignored — gospore-Go
//     handlers carry deterministic names (arg0/arg1/...), but scripts and
//     externally-imported schemas may use user-chosen names. Wire format
//     never carries parameter names, so they are not part of identity.
//   - Returns: same arity; each return Type compared via DeepEqual.
//   - HasError: must match.
//
// The check is symmetric: DescConsistent(a, b) and DescConsistent(b, a)
// always agree. Argument names "existing" and "incoming" reflect the
// expected use site (Register comparing a new descriptor against the
// already-stored one) but do not bias the comparison itself.
func DescConsistent(existing, incoming tschema.CallableDesc) error {
	if existing.Name != incoming.Name {
		return fmt.Errorf("%w: callID differs (existing=%q, incoming=%q)",
			ErrSignatureMismatch, existing.Name, incoming.Name)
	}
	if existing.Mode != incoming.Mode {
		return fmt.Errorf("%w: callable mode differs (existing=%q, incoming=%q)",
			ErrSignatureMismatch, existing.Mode, incoming.Mode)
	}
	if !reflect.DeepEqual(existing.Streaming, incoming.Streaming) {
		return fmt.Errorf("%w: streaming descriptor differs (existing=%+v, incoming=%+v)",
			ErrSignatureMismatch, existing.Streaming, incoming.Streaming)
	}
	if len(existing.Parameters) != len(incoming.Parameters) {
		return fmt.Errorf("%w: parameter arity differs (existing=%d, incoming=%d)",
			ErrSignatureMismatch, len(existing.Parameters), len(incoming.Parameters))
	}
	for i := range existing.Parameters {
		if !reflect.DeepEqual(existing.Parameters[i].Type, incoming.Parameters[i].Type) {
			return fmt.Errorf("%w: parameter %d type differs (existing=%s, incoming=%s)",
				ErrSignatureMismatch, i, existing.Parameters[i].Type, incoming.Parameters[i].Type)
		}
	}
	if len(existing.Returns) != len(incoming.Returns) {
		return fmt.Errorf("%w: return arity differs (existing=%d, incoming=%d)",
			ErrSignatureMismatch, len(existing.Returns), len(incoming.Returns))
	}
	for i := range existing.Returns {
		if !reflect.DeepEqual(existing.Returns[i], incoming.Returns[i]) {
			return fmt.Errorf("%w: return %d type differs (existing=%s, incoming=%s)",
				ErrSignatureMismatch, i, existing.Returns[i], incoming.Returns[i])
		}
	}
	if existing.HasError != incoming.HasError {
		return fmt.Errorf("%w: HasError differs (existing=%v, incoming=%v)",
			ErrSignatureMismatch, existing.HasError, incoming.HasError)
	}
	return nil
}

// ModeConsistent reports whether two HandlerMode values match. Returns
// nil on match, otherwise an error wrapping ErrModeMismatch.
//
// Trivial equality, but exposed as a named function so the Register
// entry point can map the two failure shapes (ErrSignatureMismatch /
// ErrModeMismatch) onto distinct wire diags without inline type checks.
func ModeConsistent(existing, incoming actor.HandlerMode) error {
	if existing != incoming {
		return fmt.Errorf("%w: existing=%d, incoming=%d", ErrModeMismatch, existing, incoming)
	}
	return nil
}

// VisibilityConsistent reports whether two Visibility values match.
// Returns nil on match, otherwise an error wrapping ErrVisibilityMismatch.
func VisibilityConsistent(existing, incoming actor.Visibility) error {
	if existing != incoming {
		return fmt.Errorf("%w: existing=%q, incoming=%q", ErrVisibilityMismatch, existing.String(), incoming.String())
	}
	return nil
}

// LoopConsistent reports whether two logical loop routes match.
// Returns nil on match, otherwise an error wrapping ErrLoopMismatch.
func LoopConsistent(existing, incoming string) error {
	if existing != incoming {
		return fmt.Errorf("%w: existing=%q, incoming=%q", ErrLoopMismatch, existing, incoming)
	}
	return nil
}
