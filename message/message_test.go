package message

import (
	"bytes"
	"errors"
	"testing"
)

func TestMarshalUnmarshal_Minimal(t *testing.T) {
	src := Frame{Kind: KindEnd, CorID: 1}
	wire, err := Marshal(src)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := Unmarshal(wire)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Kind != KindEnd || got.CorID != 1 {
		t.Fatalf("round-trip: got %+v", got)
	}
}

func TestMarshalUnmarshal_FullFrame(t *testing.T) {
	src := Frame{
		Kind:     KindCall,
		CorID:    42,
		Seq:      7,
		CallID:   "auth.Login",
		SchemaNS: "auth",
		SchemaID: 99,
		Body:     []byte{0x01, 0x02},
		Headers:  map[string]string{"trace": "x"},
	}
	wire, err := Marshal(src)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := Unmarshal(wire)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Kind != src.Kind {
		t.Errorf("Kind: got %d, want %d", got.Kind, src.Kind)
	}
	if got.CorID != src.CorID {
		t.Errorf("CorID: got %d, want %d", got.CorID, src.CorID)
	}
	if got.Seq != src.Seq {
		t.Errorf("Seq: got %d, want %d", got.Seq, src.Seq)
	}
	if got.CallID != src.CallID {
		t.Errorf("CallID: got %q, want %q", got.CallID, src.CallID)
	}
	if got.SchemaNS != src.SchemaNS {
		t.Errorf("SchemaNS: got %q, want %q", got.SchemaNS, src.SchemaNS)
	}
	if got.SchemaID != src.SchemaID {
		t.Errorf("SchemaID: got %d, want %d", got.SchemaID, src.SchemaID)
	}
	if !bytes.Equal(got.Body, src.Body) {
		t.Errorf("Body: got %x, want %x", got.Body, src.Body)
	}
	if got.Headers["trace"] != "x" {
		t.Errorf("Headers: got %v", got.Headers)
	}
}

func TestMarshalUnmarshal_EmptyHeaders(t *testing.T) {
	src := Frame{Kind: KindReply, CorID: 1}
	wire, err := Marshal(src)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := Unmarshal(wire)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Headers != nil {
		t.Errorf("Headers should be nil for empty input, got %v", got.Headers)
	}
}

func TestUnmarshal_Truncated(t *testing.T) {
	cases := [][]byte{
		nil,
		{},
		{0x01, 0x02, 0x03},
	}
	for _, data := range cases {
		_, err := Unmarshal(data)
		if !errors.Is(err, ErrFrameTruncated) {
			t.Errorf("Unmarshal(%x): got %v, want ErrFrameTruncated", data, err)
		}
	}
}

func TestFrameKind_String(t *testing.T) {
	cases := []struct {
		k    FrameKind
		want string
	}{
		{KindCall, "call"},
		{KindReply, "reply"},
		{KindError, "error"},
		{KindEnd, "end"},
		{KindCancel, "cancel"},
		{KindSystem, "system"},
		{FrameKind(0), ""},
		{FrameKind(99), ""},
	}
	for _, c := range cases {
		if got := c.k.String(); got != c.want {
			t.Errorf("FrameKind(%d).String() = %q, want %q", c.k, got, c.want)
		}
	}
}

func TestDiagConstants(t *testing.T) {
	if DiagStreamCancelled != "gospore.stream.cancelled" {
		t.Errorf("DiagStreamCancelled = %q", DiagStreamCancelled)
	}
	if DiagStreamPeerClosed != "gospore.stream.peer_closed" {
		t.Errorf("DiagStreamPeerClosed = %q", DiagStreamPeerClosed)
	}
	if DiagStreamModeLocked != "gospore.stream.mode_locked" {
		t.Errorf("DiagStreamModeLocked = %q", DiagStreamModeLocked)
	}
}

func TestMarshal_HeaderDeterminism(t *testing.T) {
	a := Frame{Kind: KindCall, CorID: 1, Headers: map[string]string{"a": "1", "b": "2", "c": "3"}}
	b := Frame{Kind: KindCall, CorID: 1, Headers: map[string]string{"c": "3", "a": "1", "b": "2"}}
	wa, _ := Marshal(a)
	wb, _ := Marshal(b)
	if !bytes.Equal(wa, wb) {
		t.Fatal("same headers in different order should produce same wire bytes")
	}
}
