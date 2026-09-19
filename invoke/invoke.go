package invoke

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/qomos-w/gospore/codec"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/message"
	"github.com/qomos-w/gospore/schema"
)

// SendFunc sends a Frame. The invoke pipeline accepts a closure rather than
// importing transport.Transport directly, breaking the import cycle
// (invoke → transport → mailbox → ref → invoke). The caller wraps their
// transport.Send in this closure.
type SendFunc func(frame message.Frame) error

// Invoke allocates a CorID, registers a pending slot, constructs a Call
// frame, and sends it. Per ARCHITECTURE.md §5.4:
//
//	1. Allocate CorID (App-global atomic increment).
//	2. Register in invoke table (CorID → clientStream channel).
//	3. Construct Call frame, send via send closure.
//	4. Return Stream backed by the registered channel.
//
// mode (tell/unary/stream) feeds the pending table's in-flight breakdown;
// pass the empty string when the caller does not know it. If send fails,
// the pending slot is cleaned up and the error is returned immediately.
func Invoke(caller, target id.ActorID, callID string, mode CallMode, body []byte, send SendFunc, gen *id.CorIDGenerator, pt *PendingTable) (Stream, error) {
	corID := uint64(gen.Next())
	ch := make(chan message.Frame, 256)
	pt.Register(corID, callID, string(mode), ch)

	frame := message.Frame{
		From:   caller,
		To:     target,
		Kind:   message.KindCall,
		CorID:  corID,
		CallID: callID,
		Body:   body,
	}

	if err := send(frame); err != nil {
		pt.SendFailed(corID)
		return nil, err
	}

	st := &streamImpl{ch: ch, corID: corID, pt: pt, done: make(chan struct{})}
	st.pump = pt.pumpFor(corID)
	return st, nil
}

// TypedDecode decodes one raw-reply payload (schemaID, encoding, body) into
// the expected concrete Go value. ok=false reports that the typed decode is
// unavailable or failed; Recv then falls back to the generic DecodeByID so
// the caller still sees a value rather than a hard error.
type TypedDecode func(schemaID uint64, encoding message.Encoding, body []byte) (any, bool)

// streamImpl implements Stream by reading response frames from a
// registered channel. Recv decodes reply frames by SchemaID when the
// codec has a resolver; otherwise it degrades to raw []byte.
type streamImpl struct {
	ch           chan message.Frame
	corID        uint64
	pt           *PendingTable
	codec        codec.Codec
	typedDecode  TypedDecode // optional: decode raw replies into the expected concrete Go type (avoids map[string]any)
	closeOnce    sync.Once
	done         chan struct{} // closed on Close/Cancel to unblock waiting Recv
	cancelled    atomic.Bool   // set by Cancel so Recv can distinguish caller cancellation from a legitimate void End
	// pump moves frames parked in the slot's overflow (delivered while the
	// buffer was full) back into ch. Called after every successful receive:
	// the receive freed a buffer slot, so stranded frames can advance.
	pump func()
	// terminal latches the first consumed terminal frame (KindEnd/KindError).
	// Deliver deregisters the slot on the terminal frame and never closes
	// s.ch, so a later Recv (Final's drain loop after Next consumed the
	// terminal) would otherwise select on an open, forever-empty channel pair
	// and park until ctx cancellation. Replaying the latched terminal keeps
	// repeated reads terminal-sticky instead of hanging.
	terminal atomic.Pointer[terminalResult]
}

// terminalResult is the cached outcome of a consumed terminal frame.
type terminalResult struct {
	val any
	err error
}

func (s *streamImpl) Recv() (any, error) {
	if t := s.terminal.Load(); t != nil {
		return t.val, t.err
	}
	select {
	case frame, ok := <-s.ch:
		if !ok {
			return nil, io.EOF
		}
		if s.pump != nil {
			s.pump()
		}
		v, err := s.decodeFrame(frame)
		if frame.Kind == message.KindEnd || frame.Kind == message.KindError {
			s.terminal.Store(&terminalResult{val: v, err: err})
		}
		return v, err
	case <-s.done:
		// A terminal frame may have raced with Cancel/Close; prefer it so
		// the callee's real outcome is not masked by the teardown.
		select {
		case frame, ok := <-s.ch:
			if !ok {
				return nil, io.EOF
			}
			return s.decodeFrame(frame)
		default:
		}
		if s.cancelled.Load() {
			return nil, ErrCallCancelled
		}
		return nil, io.EOF
	}
}

func (s *streamImpl) decodeFrame(frame message.Frame) (any, error) {
	switch frame.Kind {
	case message.KindReply:
		switch frame.PayloadMode {
		case message.PayloadModeValue:
			switch frame.SchemaID {
			case schema.BuiltinVoid:
				if len(frame.Body) == 0 {
					return nil, nil
				}
				return frame.Body, nil
			default:
				return frame.Body, nil
			}
		case message.PayloadModeRaw:
			if s.codec == nil {
				return nil, fmt.Errorf("invoke: raw reply requires codec")
			}
			// If the concrete Go type is known, decode directly into it to avoid
			// the generic map[string]any intermediate form.
			if s.typedDecode != nil {
				if v, ok := s.typedDecode(frame.SchemaID, frame.Encoding, frame.Body); ok {
					return v, nil
				}
				// Typed decode failed — fall through to generic decode so the caller
				// still sees a value rather than a hard error.
			}
			decoded, err := s.codec.DecodeByID(frame.SchemaID, frame.Encoding, frame.Body)
			if err != nil {
				return nil, err
			}
			return decoded, nil
		default:
			return nil, fmt.Errorf("invoke: unexpected payload mode %d", frame.PayloadMode)
		}
	case message.KindError:
		return nil, errors.New(string(frame.Body))
	case message.KindEnd:
		return nil, io.EOF
	}
	return nil, fmt.Errorf("invoke: unexpected frame kind %d", frame.Kind)
}

func (s *streamImpl) RecvRaw() ([]byte, error) {
	if t := s.terminal.Load(); t != nil {
		return nil, t.err
	}
	select {
	case frame, ok := <-s.ch:
		if !ok {
			return nil, io.EOF
		}
		if s.pump != nil {
			s.pump()
		}
		raw, err := s.decodeRawFrame(frame)
		if frame.Kind == message.KindEnd || frame.Kind == message.KindError {
			v := any(raw)
			if err != nil {
				v = nil
			}
			s.terminal.Store(&terminalResult{val: v, err: err})
		}
		return raw, err
	case <-s.done:
		// A terminal frame may have raced with Cancel/Close; prefer it so
		// the callee's real outcome is not masked by the teardown.
		select {
		case frame, ok := <-s.ch:
			if !ok {
				return nil, io.EOF
			}
			return s.decodeRawFrame(frame)
		default:
		}
		if s.cancelled.Load() {
			return nil, ErrCallCancelled
		}
		return nil, io.EOF
	}
}

func (s *streamImpl) decodeRawFrame(frame message.Frame) ([]byte, error) {
	switch frame.Kind {
	case message.KindReply:
		return frame.Body, nil
	case message.KindError:
		return nil, errors.New(string(frame.Body))
	case message.KindEnd:
		return nil, io.EOF
	}
	return nil, fmt.Errorf("invoke: unexpected frame kind %d", frame.Kind)
}

func (s *streamImpl) Close() error {
	s.closeOnce.Do(func() {
		s.pt.Unregister(s.corID)
		close(s.done)
	})
	return nil
}

// Cancel unregisters the pending slot and closes s.done to unblock any
// waiting Recv. It marks the stream cancelled first so Recv distinguishes
// caller cancellation (ErrCallCancelled) from a legitimate void-handler
// End (io.EOF) — without this, gateway timeouts are masked as empty
// success replies. Idempotent.
func (s *streamImpl) Cancel() error {
	s.cancelled.Store(true)
	s.closeOnce.Do(func() {
		s.pt.Unregister(s.corID)
		close(s.done)
	})
	return nil
}

// NewStream wraps a pending channel into a Stream. Used by the app layer
// when it performs direct in-process delivery (bypassing invoke.Invoke
// to set Sender on the envelope).
func NewStream(ch chan message.Frame, corID uint64, pt *PendingTable, c codec.Codec) Stream {
	st := &streamImpl{ch: ch, corID: corID, pt: pt, codec: c, done: make(chan struct{})}
	st.pump = pt.pumpFor(corID)
	return st
}

// NewStreamWithTypedDecode is like NewStream but installs a typed decode
// hook for PayloadModeRaw replies. When td is non-nil, Recv decodes
// directly into the concrete Go type it produces, yielding a typed struct
// instead of the generic map[string]any produced by DecodeByID; on typed
// failure Recv falls back to the generic path. Use NewStreamAs when the
// reply type is known at compile time.
func NewStreamWithTypedDecode(ch chan message.Frame, corID uint64, pt *PendingTable, c codec.Codec, td TypedDecode) Stream {
	st := &streamImpl{ch: ch, corID: corID, pt: pt, codec: c, typedDecode: td, done: make(chan struct{})}
	st.pump = pt.pumpFor(corID)
	return st
}

// NewStreamAs is NewStreamWithTypedDecode with the reply type supplied as a
// type parameter: Recv decodes PayloadModeRaw frames straight into T via
// codec.DecodeAs, with no reflection. When the typed decode fails, Recv
// falls back to generic DecodeByID so the caller still sees a value.
func NewStreamAs[T any](ch chan message.Frame, corID uint64, pt *PendingTable, c codec.Codec) Stream {
	return NewStreamWithTypedDecode(ch, corID, pt, c, func(schemaID uint64, encoding message.Encoding, body []byte) (any, bool) {
		v, err := codec.DecodeAs[T](c, schemaID, encoding, body)
		if err != nil {
			return nil, false
		}
		return v, true
	})
}

// errorStream is a Stream that immediately returns an error on every
// read operation. Used when in-process delivery fails before a channel
// can be allocated.
type errorStream struct{ err error }

func (s *errorStream) Recv() (any, error)       { return nil, s.err }
func (s *errorStream) RecvRaw() ([]byte, error) { return nil, s.err }
func (s *errorStream) Close() error             { return nil }

// NewErrorStream returns a Stream that always returns err on Recv /
// RecvRaw. Close is a no-op.
func NewErrorStream(err error) Stream {
	return &errorStream{err: err}
}
