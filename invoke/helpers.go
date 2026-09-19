package invoke

import (
	"errors"
	"io"
)

// ErrUnaryExpected is returned by Once when a Stream yields more than one
// value frame before io.EOF. The protocol is in violation of unary
// semantics; the first value has been discarded because it is no longer
// safe to attribute meaning to it.
var ErrUnaryExpected = errors.New("gospore.invoke.unary_violation: stream returned multiple frames where one was expected")

// Once consumes exactly one value frame from s and verifies the next Recv
// reports io.EOF. It is the canonical caller helper for unary callables:
// callers receive (value, nil) on success or (nil, err) on any anomaly,
// without writing the io.EOF sentinel check themselves.
//
// Error semantics:
//   - First Recv returns io.EOF: returned verbatim. The callable produced
//     no value (legal for fire-and-forget; ambiguous for unary — caller
//     decides).
//   - First Recv returns any other error: returned verbatim.
//   - First Recv returns a value, second Recv returns io.EOF: success;
//     (value, nil).
//   - First Recv returns a value, second Recv returns a non-EOF error:
//     the error is returned and the first value is discarded.
//   - First Recv returns a value, second Recv also returns a value:
//     ErrUnaryExpected. The first value is discarded.
//
// Once does NOT Close s. The caller retains ownership and must Close
// after consumption.
func Once(s Stream) (any, error) {
	v, err := s.Recv()
	if err != nil {
		return nil, err
	}
	_, err2 := s.Recv()
	if errors.Is(err2, io.EOF) {
		return v, nil
	}
	if err2 != nil {
		return nil, err2
	}
	return nil, ErrUnaryExpected
}

// OnceRaw is the RecvRaw counterpart to Once. Identical semantics; returns
// raw Frame.Body bytes instead of decoded values. Mutual-exclusion with
// Recv applies — if any Recv has been called on s, OnceRaw will surface
// gospore.stream.mode_locked from the underlying stream.
func OnceRaw(s Stream) ([]byte, error) {
	v, err := s.RecvRaw()
	if err != nil {
		return nil, err
	}
	_, err2 := s.RecvRaw()
	if errors.Is(err2, io.EOF) {
		return v, nil
	}
	if err2 != nil {
		return nil, err2
	}
	return nil, ErrUnaryExpected
}

// Each iterates s until io.EOF, invoking fn for every value frame. It is
// the canonical caller helper for streaming callables. Iteration stops
// (and the corresponding error is returned) when:
//   - fn returns a non-nil error — that error is returned verbatim;
//   - Recv returns a non-EOF error — that error is returned verbatim;
//   - Recv returns io.EOF — Each returns nil (normal completion).
//
// Each does NOT Close s. The caller retains ownership.
func Each(s Stream, fn func(any) error) error {
	for {
		v, err := s.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := fn(v); err != nil {
			return err
		}
	}
}

// EachRaw is the RecvRaw counterpart to Each. Identical semantics; fn
// receives raw Frame.Body bytes instead of decoded values. Mutual-exclusion
// with Recv applies as in OnceRaw.
func EachRaw(s Stream, fn func([]byte) error) error {
	for {
		v, err := s.RecvRaw()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := fn(v); err != nil {
			return err
		}
	}
}
