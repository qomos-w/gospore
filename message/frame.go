package message

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/internal/collections"
	"github.com/qomos-w/spore/identity"
)

// PayloadMode declares whether Frame.Body is the value itself or an already
// codec-serialised byte stream.
type PayloadMode uint8

const (
	PayloadModeValue PayloadMode = iota
	PayloadModeRaw
)

func (m PayloadMode) String() string {
	switch m {
	case PayloadModeValue:
		return "value"
	case PayloadModeRaw:
		return "raw"
	default:
		return ""
	}
}

// Encoding declares which codec representation produced Frame.Body when the
// payload mode is Raw. Value payloads use EncodingNone.
type Encoding uint8

const (
	EncodingNone Encoding = iota
	EncodingJSON
	EncodingBinary
)

func (e Encoding) String() string {
	switch e {
	case EncodingNone:
		return "none"
	case EncodingJSON:
		return "json"
	case EncodingBinary:
		return "binary"
	default:
		return ""
	}
}

// Frame is the on-wire unit of communication between actors.
//
// CallID is non-empty only for Call and System frames; Reply / End / Error
// / Cancel pair to a Call solely via CorID. SchemaNS + SchemaID identify
// the schema of Body, allowing each frame's body to use its own namespace
// independent of the originating Call.
//
// PayloadMode distinguishes whether Body is the value itself (Value) or a
// codec-serialised byte stream (Raw). Encoding is meaningful only for Raw.
//
// Headers is a transport-layer extension point (trace-id, retry-count,
// token hint, etc.) — handlers do NOT read or write Headers; they read
// caller-derived state via Context.Identity().
type Frame struct {
	From        id.ActorID
	To          id.ActorID
	Kind        FrameKind
	CorID       uint64
	Seq         uint32
	CallID      string
	SchemaNS    string
	SchemaID    uint64
	PayloadMode PayloadMode
	Encoding    Encoding
	Body        []byte
	Headers     map[string]string
}

// ErrFrameTruncated is returned by Unmarshal when input ends before a
// complete Frame can be reconstructed.
var ErrFrameTruncated = errors.New("gospore/message: truncated frame")

// ErrFrameMalformed is returned when a frame decodes structurally but its
// payload-mode / encoding combination is invalid for the protocol.
var ErrFrameMalformed = errors.New("gospore/message: malformed frame")

func validateFrame(f Frame) error {
	switch f.Kind {
	case KindReply, KindCall, KindSystem:
		switch f.PayloadMode {
		case PayloadModeValue:
			if f.Encoding != EncodingNone {
				return fmt.Errorf("%w: value payload requires encoding none", ErrFrameMalformed)
			}
		case PayloadModeRaw:
			if f.Encoding != EncodingJSON && f.Encoding != EncodingBinary {
				return fmt.Errorf("%w: raw payload requires json or binary encoding", ErrFrameMalformed)
			}
		default:
			return fmt.Errorf("%w: unknown payload mode %d", ErrFrameMalformed, f.PayloadMode)
		}
	default:
		if f.PayloadMode != PayloadModeValue || f.Encoding != EncodingNone {
			return fmt.Errorf("%w: non-payload frame must use zero-value payload metadata", ErrFrameMalformed)
		}
	}
	return nil
}

// Marshal encodes Frame to its canonical byte sequence. Header keys are
// emitted in sorted order so the output is byte-stable for a given Frame.
func Marshal(f Frame) ([]byte, error) {
	if err := validateFrame(f); err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	from := f.From.Canonical()
	to := f.To.Canonical()
	buf.Write(from[:])
	buf.Write(to[:])

	buf.WriteByte(byte(f.Kind))

	var fixed [12]byte
	binary.BigEndian.PutUint64(fixed[0:8], f.CorID)
	binary.BigEndian.PutUint32(fixed[8:12], f.Seq)
	buf.Write(fixed[:])

	writeBytes(&buf, []byte(f.CallID))
	writeBytes(&buf, []byte(f.SchemaNS))

	var schemaIDBuf [binary.MaxVarintLen64]byte
	sn := binary.PutUvarint(schemaIDBuf[:], f.SchemaID)
	buf.Write(schemaIDBuf[:sn])
	buf.WriteByte(byte(f.PayloadMode))
	buf.WriteByte(byte(f.Encoding))

	writeBytes(&buf, f.Body)

	keys := collections.SortedKeys(f.Headers)

	var hdrCount [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(hdrCount[:], uint64(len(keys)))
	buf.Write(hdrCount[:n])

	for _, k := range keys {
		writeBytes(&buf, []byte(k))
		writeBytes(&buf, []byte(f.Headers[k]))
	}

	return buf.Bytes(), nil
}

// Unmarshal decodes a Frame from its canonical byte sequence. A truncated
// or malformed input returns ErrFrameTruncated / ErrFrameMalformed.
func Unmarshal(data []byte) (Frame, error) {
	var f Frame
	r := bytes.NewReader(data)

	var from, to [16]byte
	if _, err := io.ReadFull(r, from[:]); err != nil {
		return f, ErrFrameTruncated
	}
	if _, err := io.ReadFull(r, to[:]); err != nil {
		return f, ErrFrameTruncated
	}
	f.From = id.From(identity.CanonicalID(from))
	f.To = id.From(identity.CanonicalID(to))

	kind, err := r.ReadByte()
	if err != nil {
		return f, ErrFrameTruncated
	}
	f.Kind = FrameKind(kind)

	var fixed [12]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return f, ErrFrameTruncated
	}
	f.CorID = binary.BigEndian.Uint64(fixed[0:8])
	f.Seq = binary.BigEndian.Uint32(fixed[8:12])

	callID, err := readBytes(r)
	if err != nil {
		return f, ErrFrameTruncated
	}
	f.CallID = string(callID)

	schemaNS, err := readBytes(r)
	if err != nil {
		return f, ErrFrameTruncated
	}
	f.SchemaNS = string(schemaNS)

	schemaID, err := binary.ReadUvarint(r)
	if err != nil {
		return f, ErrFrameTruncated
	}
	f.SchemaID = schemaID
	payloadMode, err := r.ReadByte()
	if err != nil {
		return f, ErrFrameTruncated
	}
	f.PayloadMode = PayloadMode(payloadMode)
	encoding, err := r.ReadByte()
	if err != nil {
		return f, ErrFrameTruncated
	}
	f.Encoding = Encoding(encoding)

	body, err := readBytes(r)
	if err != nil {
		return f, ErrFrameTruncated
	}
	f.Body = body

	hdrCount, err := binary.ReadUvarint(r)
	if err != nil {
		return f, ErrFrameTruncated
	}
	if hdrCount > 0 {
		f.Headers = make(map[string]string, hdrCount)
		for i := uint64(0); i < hdrCount; i++ {
			k, err := readBytes(r)
			if err != nil {
				return f, ErrFrameTruncated
			}
			v, err := readBytes(r)
			if err != nil {
				return f, ErrFrameTruncated
			}
			f.Headers[string(k)] = string(v)
		}
	}
	if err := validateFrame(f); err != nil {
		return Frame{}, err
	}

	return f, nil
}

// writeBytes emits a uvarint length prefix followed by the raw bytes.
func writeBytes(buf *bytes.Buffer, data []byte) {
	var sz [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(sz[:], uint64(len(data)))
	buf.Write(sz[:n])
	buf.Write(data)
}

// readBytes consumes a uvarint length prefix and returns the following
// bytes; a zero length yields nil (no allocation).
func readBytes(r *bytes.Reader) ([]byte, error) {
	sz, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	if sz == 0 {
		return nil, nil
	}
	out := make([]byte, sz)
	if _, err := io.ReadFull(r, out); err != nil {
		return nil, err
	}
	return out, nil
}
