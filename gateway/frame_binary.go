package gateway

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/qomos-w/gospore/internal/collections"
)

var (
	ErrFrameTruncated = errors.New("gateway: truncated frame")
	ErrFrameMalformed = errors.New("gateway: malformed frame")
)

const (
	binaryFrameMagic   = "GSF\x01"
	binaryFrameVersion = 2
)

// PayloadEncoding declares how the Payload body is encoded.
type PayloadEncoding uint8

const (
	EncodingJSON PayloadEncoding = iota
	EncodingBinary
)

// PayloadCompression declares whether the Payload is compressed.
type PayloadCompression uint8

const (
	CompressionNone PayloadCompression = iota
	CompressionGzip
)

// FrameType discriminates the semantic meaning of a wire frame.
type FrameType uint8

const (
	FrameTypeInvoke FrameType = 1 + iota
	FrameTypeReply
	FrameTypeError
	FrameTypeChunk
	FrameTypeEnd
	FrameTypeSubscribe
	FrameTypeUnsubscribe
	FrameTypeAuth
	FrameTypeAuthOk
	FrameTypePing
	FrameTypePong
)

// Flags layout: bits 0-1 = PayloadEncoding, bits 2-3 = PayloadCompression, bits 4-7 = reserved.
type Flags uint8

func (f Flags) Encoding() PayloadEncoding       { return PayloadEncoding(f & 0x03) }
func (f Flags) Compression() PayloadCompression { return PayloadCompression((f >> 2) & 0x03) }

// MakeFlags packs encoding and compression into a single flag byte.
func MakeFlags(enc PayloadEncoding, comp PayloadCompression) Flags {
	return Flags(uint8(enc) | (uint8(comp) << 2))
}

// WireFrame is the on-wire unit exchanged between the client and the gateway
// over a WebSocket binary message.
type WireFrame struct {
	Flags    Flags
	Type     FrameType
	CorID    uint64
	Seq      uint32
	TransID  uint64
	CallID   string
	SubID    string
	Target   string
	From     string
	ErrorMsg string
	Payload  []byte
	Headers  map[string]string
}

// MarshalWireFrame encodes f to its canonical binary form.
func MarshalWireFrame(f *WireFrame) ([]byte, error) {
	var buf bytes.Buffer
	marshalWireFrameTo(&buf, f)
	return buf.Bytes(), nil
}

// marshalWireFrameTo encodes f into buf without allocating a new buffer,
// letting callers pool bytes.Buffer instances across frames.
func marshalWireFrameTo(buf *bytes.Buffer, f *WireFrame) {
	buf.WriteString(binaryFrameMagic)
	buf.WriteByte(binaryFrameVersion)
	buf.WriteByte(byte(f.Flags))
	buf.WriteByte(byte(f.Type))

	var fixed [20]byte
	binary.BigEndian.PutUint64(fixed[0:8], f.CorID)
	binary.BigEndian.PutUint32(fixed[8:12], f.Seq)
	binary.BigEndian.PutUint64(fixed[12:20], f.TransID)
	buf.Write(fixed[:])

	writeUvarintString(buf, f.CallID)
	writeUvarintString(buf, f.SubID)
	writeUvarintString(buf, f.Target)
	writeUvarintString(buf, f.ErrorMsg)
	writeUvarintBytes(buf, f.Payload)

	keys := collections.SortedKeys(f.Headers)
	writeUvarint(buf, uint64(len(keys)))
	for _, k := range keys {
		writeUvarintString(buf, k)
		writeUvarintString(buf, f.Headers[k])
	}

	// From is appended at the end so old receivers that stop reading after
	// Headers can ignore it, and new receivers can still recover when it is
	// absent (backward compatibility with version-1 frames).
	writeUvarintString(buf, f.From)
}

// UnmarshalWireFrame decodes a WireFrame from its canonical binary form.
func UnmarshalWireFrame(data []byte) (*WireFrame, error) {
	r := bytes.NewReader(data)

	var magic [4]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return nil, ErrFrameTruncated
	}
	if string(magic[:]) != binaryFrameMagic {
		return nil, fmt.Errorf("%w: invalid magic", ErrFrameMalformed)
	}

	version, err := r.ReadByte()
	if err != nil {
		return nil, ErrFrameTruncated
	}
	if version != binaryFrameVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrFrameMalformed, version)
	}

	flagsByte, err := r.ReadByte()
	if err != nil {
		return nil, ErrFrameTruncated
	}

	typeByte, err := r.ReadByte()
	if err != nil {
		return nil, ErrFrameTruncated
	}

	f := &WireFrame{
		Flags: Flags(flagsByte),
		Type:  FrameType(typeByte),
	}

	var fixed [20]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return nil, ErrFrameTruncated
	}
	f.CorID = binary.BigEndian.Uint64(fixed[0:8])
	f.Seq = binary.BigEndian.Uint32(fixed[8:12])
	f.TransID = binary.BigEndian.Uint64(fixed[12:20])

	f.CallID, err = readUvarintString(r)
	if err != nil {
		return nil, err
	}
	f.SubID, err = readUvarintString(r)
	if err != nil {
		return nil, err
	}
	f.Target, err = readUvarintString(r)
	if err != nil {
		return nil, err
	}
	f.ErrorMsg, err = readUvarintString(r)
	if err != nil {
		return nil, err
	}
	f.Payload, err = readUvarintBytes(r)
	if err != nil {
		return nil, err
	}

	hdrCount, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, ErrFrameTruncated
	}
	if hdrCount > 0 {
		f.Headers = make(map[string]string, hdrCount)
		for i := uint64(0); i < hdrCount; i++ {
			k, err := readUvarintString(r)
			if err != nil {
				return nil, err
			}
			v, err := readUvarintString(r)
			if err != nil {
				return nil, err
			}
			f.Headers[k] = v
		}
	}

	// From is optional for backward compatibility with version-1 frames.
	if r.Len() > 0 {
		f.From, _ = readUvarintString(r)
	}

	return f, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func writeUvarint(buf *bytes.Buffer, v uint64) {
	var b [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(b[:], v)
	buf.Write(b[:n])
}

func writeUvarintBytes(buf *bytes.Buffer, data []byte) {
	writeUvarint(buf, uint64(len(data)))
	buf.Write(data)
}

func writeUvarintString(buf *bytes.Buffer, s string) {
	writeUvarintBytes(buf, []byte(s))
}

func readUvarintString(r *bytes.Reader) (string, error) {
	b, err := readUvarintBytes(r)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func readUvarintBytes(r *bytes.Reader) ([]byte, error) {
	sz, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, ErrFrameTruncated
	}
	if sz == 0 {
		return nil, nil
	}
	out := make([]byte, sz)
	if _, err := io.ReadFull(r, out); err != nil {
		return nil, ErrFrameTruncated
	}
	return out, nil
}
