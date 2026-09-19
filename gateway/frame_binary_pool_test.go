package gateway

import (
	"bytes"
	"testing"
)

func TestMarshalWireFrameToMatchesMarshalWireFrame(t *testing.T) {
	f := &WireFrame{
		Flags:    MakeFlags(EncodingJSON, CompressionGzip),
		Type:     FrameTypeReply,
		CorID:    42,
		Seq:      7,
		TransID:  99,
		CallID:   "test.call",
		SubID:    "sub-1",
		Target:   "svc/actor",
		From:     "src/actor",
		ErrorMsg: "",
		Payload:  []byte("payload-bytes"),
		Headers:  map[string]string{"b": "2", "a": "1"},
	}

	want, err := MarshalWireFrame(f)
	if err != nil {
		t.Fatalf("MarshalWireFrame: %v", err)
	}

	buf := new(bytes.Buffer)
	marshalWireFrameTo(buf, f)
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("marshalWireFrameTo output differs from MarshalWireFrame")
	}
}

func TestMarshalWireFrameToBufferReuse(t *testing.T) {
	big := &WireFrame{Type: FrameTypeReply, CorID: 1, Payload: bytes.Repeat([]byte("x"), 4096), From: "a"}
	small := &WireFrame{Type: FrameTypeReply, CorID: 2, From: "b"}

	var buf bytes.Buffer
	marshalWireFrameTo(&buf, big)
	wantBig, _ := MarshalWireFrame(big)
	if !bytes.Equal(buf.Bytes(), wantBig) {
		t.Fatal("first frame mismatch")
	}

	// Reset + re-encode into the same backing array: the second frame must
	// not contain stale bytes from the first.
	buf.Reset()
	marshalWireFrameTo(&buf, small)
	wantSmall, _ := MarshalWireFrame(small)
	if !bytes.Equal(buf.Bytes(), wantSmall) {
		t.Fatalf("second frame mismatch after buffer reuse: got %d bytes, want %d", buf.Len(), len(wantSmall))
	}
}
