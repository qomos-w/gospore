package gateway

import (
	"bytes"
	"testing"
)

func TestMarshalUnmarshal_Minimal(t *testing.T) {
	src := &WireFrame{
		Flags: MakeFlags(EncodingJSON, CompressionNone),
		Type:  FrameTypePing,
		CorID: 1,
	}
	wire, err := MarshalWireFrame(src)
	if err != nil {
		t.Fatalf("MarshalWireFrame: %v", err)
	}
	got, err := UnmarshalWireFrame(wire)
	if err != nil {
		t.Fatalf("UnmarshalWireFrame: %v", err)
	}
	if got.Type != src.Type || got.CorID != src.CorID {
		t.Fatalf("round-trip: got %+v", got)
	}
}

func TestMarshalUnmarshal_FullFrame(t *testing.T) {
	src := &WireFrame{
		Flags:    MakeFlags(EncodingBinary, CompressionGzip),
		Type:     FrameTypeInvoke,
		CorID:    42,
		Seq:      7,
		TransID:  123,
		CallID:   "filesystem.list_json",
		SubID:    "sub-123",
		Target:   "actor-target",
		ErrorMsg: "",
		Payload:  []byte{0x54, 0x42, 0x43, 0x01, 0x02},
		Headers:  map[string]string{"trace": "x", "auth": "token"},
	}
	wire, err := MarshalWireFrame(src)
	if err != nil {
		t.Fatalf("MarshalWireFrame: %v", err)
	}
	got, err := UnmarshalWireFrame(wire)
	if err != nil {
		t.Fatalf("UnmarshalWireFrame: %v", err)
	}
	if got.Flags != src.Flags {
		t.Errorf("Flags: got %d, want %d", got.Flags, src.Flags)
	}
	if got.Type != src.Type {
		t.Errorf("Type: got %d, want %d", got.Type, src.Type)
	}
	if got.CorID != src.CorID {
		t.Errorf("CorID: got %d, want %d", got.CorID, src.CorID)
	}
	if got.Seq != src.Seq {
		t.Errorf("Seq: got %d, want %d", got.Seq, src.Seq)
	}
	if got.TransID != src.TransID {
		t.Errorf("TransID: got %d, want %d", got.TransID, src.TransID)
	}
	if got.CallID != src.CallID {
		t.Errorf("CallID: got %q, want %q", got.CallID, src.CallID)
	}
	if got.SubID != src.SubID {
		t.Errorf("SubID: got %q, want %q", got.SubID, src.SubID)
	}
	if got.Target != src.Target {
		t.Errorf("Target: got %q, want %q", got.Target, src.Target)
	}
	if !bytes.Equal(got.Payload, src.Payload) {
		t.Errorf("Payload: got %x, want %x", got.Payload, src.Payload)
	}
	if got.Headers["trace"] != "x" || got.Headers["auth"] != "token" {
		t.Errorf("Headers: got %v", got.Headers)
	}
}

func TestMarshalUnmarshal_EmptyFields(t *testing.T) {
	src := &WireFrame{
		Flags:   MakeFlags(EncodingJSON, CompressionNone),
		Type:    FrameTypeReply,
		CorID:   99,
		Payload: nil,
	}
	wire, err := MarshalWireFrame(src)
	if err != nil {
		t.Fatalf("MarshalWireFrame: %v", err)
	}
	got, err := UnmarshalWireFrame(wire)
	if err != nil {
		t.Fatalf("UnmarshalWireFrame: %v", err)
	}
	if got.Headers != nil {
		t.Errorf("Headers should be nil for empty input, got %v", got.Headers)
	}
	if len(got.Payload) != 0 {
		t.Errorf("Payload should be empty, got %x", got.Payload)
	}
}

func TestUnmarshal_Truncated(t *testing.T) {
	cases := [][]byte{
		nil,
		{},
		{0x01, 0x02, 0x03},
	}
	for _, data := range cases {
		_, err := UnmarshalWireFrame(data)
		if err != ErrFrameTruncated {
			t.Errorf("UnmarshalWireFrame(%x): got %v, want ErrFrameTruncated", data, err)
		}
	}
}

func TestUnmarshal_MalformedMagic(t *testing.T) {
	data := []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01}
	_, err := UnmarshalWireFrame(data)
	if err == nil || err == ErrFrameTruncated {
		t.Errorf("expected malformed error, got %v", err)
	}
}

func TestUnmarshal_UnsupportedVersion(t *testing.T) {
	data := []byte{'G', 'S', 'F', 0x01, 0x99, 0x00, 0x01}
	_, err := UnmarshalWireFrame(data)
	if err == nil || err == ErrFrameTruncated {
		t.Errorf("expected malformed/version error, got %v", err)
	}
}

func TestMarshal_HeaderDeterminism(t *testing.T) {
	a := &WireFrame{Flags: MakeFlags(EncodingJSON, CompressionNone), Type: FrameTypeInvoke, CorID: 1, Headers: map[string]string{"a": "1", "b": "2", "c": "3"}}
	b := &WireFrame{Flags: MakeFlags(EncodingJSON, CompressionNone), Type: FrameTypeInvoke, CorID: 1, Headers: map[string]string{"c": "3", "a": "1", "b": "2"}}
	wa, _ := MarshalWireFrame(a)
	wb, _ := MarshalWireFrame(b)
	if !bytes.Equal(wa, wb) {
		t.Fatal("same headers in different order should produce same wire bytes")
	}
}

func TestFlags_RoundTrip(t *testing.T) {
	cases := []struct {
		enc  PayloadEncoding
		comp PayloadCompression
	}{
		{EncodingJSON, CompressionNone},
		{EncodingBinary, CompressionNone},
		{EncodingJSON, CompressionGzip},
		{EncodingBinary, CompressionGzip},
	}
	for _, c := range cases {
		f := MakeFlags(c.enc, c.comp)
		if f.Encoding() != c.enc {
			t.Errorf("MakeFlags(%d,%d).Encoding() = %d", c.enc, c.comp, f.Encoding())
		}
		if f.Compression() != c.comp {
			t.Errorf("MakeFlags(%d,%d).Compression() = %d", c.enc, c.comp, f.Compression())
		}
	}
}

func TestFrameType_Values(t *testing.T) {
	cases := []struct {
		ft   FrameType
		want uint8
	}{
		{FrameTypeInvoke, 1},
		{FrameTypeReply, 2},
		{FrameTypeError, 3},
		{FrameTypeChunk, 4},
		{FrameTypeEnd, 5},
		{FrameTypeSubscribe, 6},
		{FrameTypeUnsubscribe, 7},
		{FrameTypeAuth, 8},
		{FrameTypeAuthOk, 9},
		{FrameTypePing, 10},
		{FrameTypePong, 11},
	}
	for _, c := range cases {
		if uint8(c.ft) != c.want {
			t.Errorf("FrameType %d: want %d", c.ft, c.want)
		}
	}
}
