package invoke

import (
	"testing"

	"github.com/qomos-w/gospore/codec"
	"github.com/qomos-w/gospore/message"
	"github.com/qomos-w/gospore/schema"
	sporesch "github.com/qomos-w/spore/schema"
	"github.com/qomos-w/spore/transport"
)

type streamReply struct {
	Name string
	Age  int
}

// newStructCodec registers a struct schema and returns a resolver-bound
// JSON codec plus the schema's ID.
func newStructCodec(t *testing.T) (codec.Codec, uint64) {
	t.Helper()
	set, err := schema.New("invoketest")
	if err != nil {
		t.Fatalf("schema.New: %v", err)
	}
	desc := sporesch.TypeDesc{Kind: sporesch.TypeKindStruct, Name: "streamReply", ClassName: "streamReply"}
	id, err := set.RegisterAuto("streamReply", desc)
	if err != nil {
		t.Fatalf("RegisterAuto: %v", err)
	}
	return codec.NewWithResolver(&transport.JSONCodec{}, set), id
}

// NewStreamAs[T] must decode a raw reply straight into T with no
// reflection, populating struct fields directly.
func TestNewStreamAs_TypedRecv(t *testing.T) {
	c, id := newStructCodec(t)
	ch := make(chan message.Frame, 2)
	pt := NewPendingTable()
	s := NewStreamAs[streamReply](ch, 1, pt, c)

	ch <- message.Frame{
		Kind:        message.KindReply,
		PayloadMode: message.PayloadModeRaw,
		SchemaID:    id,
		Encoding:    message.EncodingJSON,
		Body:        []byte(`{"Name":"ada","Age":36}`),
	}
	v, err := s.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	got, ok := v.(streamReply)
	if !ok {
		t.Fatalf("Recv returned %T, want streamReply", v)
	}
	if got.Name != "ada" || got.Age != 36 {
		t.Fatalf("decoded %+v, want {Name:ada Age:36}", got)
	}
}

// When the typed decode fails (unknown schema ID), Recv must fall back to
// the generic DecodeByID so the caller still sees a value — here the raw
// bytes DecodeByID returns for an unresolvable schema.
func TestNewStreamAs_FallsBackToGenericDecode(t *testing.T) {
	c := codec.NewJSON()
	ch := make(chan message.Frame, 2)
	pt := NewPendingTable()
	s := NewStreamAs[streamReply](ch, 1, pt, c)

	body := []byte(`{"Name":"ada"}`)
	ch <- message.Frame{
		Kind:        message.KindReply,
		PayloadMode: message.PayloadModeRaw,
		SchemaID:    9999, // unknown to a resolver-less codec
		Encoding:    message.EncodingJSON,
		Body:        body,
	}
	v, err := s.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	got, ok := v.([]byte)
	if !ok {
		t.Fatalf("fallback returned %T, want []byte", v)
	}
	if string(got) != string(body) {
		t.Fatalf("fallback returned %q, want %q", got, body)
	}
}

// NewStreamWithTypedDecode must honour a caller-supplied hook: success
// short-circuits the generic path, failure falls back to it.
func TestNewStreamWithTypedDecode_Hook(t *testing.T) {
	c, id := newStructCodec(t)
	ch := make(chan message.Frame, 2)
	pt := NewPendingTable()
	hook := TypedDecode(func(schemaID uint64, encoding message.Encoding, body []byte) (any, bool) {
		if schemaID == id {
			return streamReply{Name: "hooked"}, true
		}
		return nil, false
	})
	s := NewStreamWithTypedDecode(ch, 1, pt, c, hook)

	ch <- message.Frame{
		Kind:        message.KindReply,
		PayloadMode: message.PayloadModeRaw,
		SchemaID:    id,
		Encoding:    message.EncodingJSON,
		Body:        []byte(`{"Name":"ignored"}`),
	}
	v, err := s.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if got, ok := v.(streamReply); !ok || got.Name != "hooked" {
		t.Fatalf("hook value not used: %#v", v)
	}

	// Hook reports failure for this schema → generic decode takes over.
	ch <- message.Frame{
		Kind:        message.KindReply,
		PayloadMode: message.PayloadModeRaw,
		SchemaID:    404,
		Encoding:    message.EncodingJSON,
		Body:        []byte(`{"other":true}`),
	}
	v, err = s.Recv()
	if err != nil {
		t.Fatalf("Recv (fallback): %v", err)
	}
	if _, ok := v.([]byte); !ok {
		t.Fatalf("fallback returned %T, want []byte", v)
	}
}
