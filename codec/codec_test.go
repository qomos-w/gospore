package codec

import (
	"testing"

	"github.com/qomos-w/gospore/message"
	"github.com/qomos-w/gospore/schema"
	sporesch "github.com/qomos-w/spore/schema"
	"github.com/qomos-w/spore/transport"
)

func TestCodec_New_PanicsOnNil(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("New(nil) should panic")
		}
	}()
	New(nil)
}

func TestCodec_NewJSON_NotNil(t *testing.T) {
	c := NewJSON()
	if c == nil {
		t.Fatal("NewJSON() returned nil")
	}
}

func TestCodec_NewBinary_NotNil(t *testing.T) {
	c := NewBinary()
	if c == nil {
		t.Fatal("NewBinary() returned nil")
	}
}

func TestCodec_JSONRoundTrip(t *testing.T) {
	c := NewJSON()
	desc := sporesch.TypeDesc{Name: "test"}

	original := map[string]any{"key": "value", "num": float64(42)}
	encoded, err := c.Encode(desc, original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) == 0 {
		t.Fatal("Encode produced empty output")
	}

	decoded, err := c.Decode(desc, encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	m, ok := decoded.(map[string]any)
	if !ok {
		t.Fatalf("decoded type %T, want map[string]any", decoded)
	}
	if m["key"] != "value" {
		t.Errorf("key: got %v, want \"value\"", m["key"])
	}
}

func TestCodec_BinaryNotNil(t *testing.T) {
	c := NewBinary()
	if c == nil {
		t.Fatal("NewBinary() returned nil")
	}
}

func TestCodec_BinaryPassthrough(t *testing.T) {
	c := NewBinary()
	desc := sporesch.TypeDesc{Name: "raw"}

	// []byte should passthrough without touching the binary backend
	payload := []byte{0x01, 0x02, 0x03}
	out, err := c.Encode(desc, payload)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if string(out) != string(payload) {
		t.Errorf("[]byte not passed through: got %x, want %x", out, payload)
	}
}

func TestCodec_EncodeBytesPassthrough(t *testing.T) {
	c := NewJSON()
	desc := sporesch.TypeDesc{Name: "pass"}

	raw := []byte{0xAB, 0xCD}
	out, err := c.Encode(desc, raw)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if string(out) != string(raw) {
		t.Errorf("[]byte not passed through: got %x, want %x", out, raw)
	}
}

type decodeAsUser struct {
	Name string
	Age  int
}

func TestCodec_DecodeAs_BuiltinTyped(t *testing.T) {
	c := NewJSON()
	got, err := DecodeAs[map[string]string](c, schema.BuiltinMapStringString, message.EncodingJSON, []byte(`{"a":"b","k":"v"}`))
	if err != nil {
		t.Fatalf("DecodeAs: %v", err)
	}
	if got["a"] != "b" || got["k"] != "v" || len(got) != 2 {
		t.Fatalf("decoded %v, want {a:b k:v}", got)
	}
}

func TestCodec_DecodeAs_StructViaResolver(t *testing.T) {
	set, err := schema.New("codectest")
	if err != nil {
		t.Fatalf("schema.New: %v", err)
	}
	desc := sporesch.TypeDesc{Kind: sporesch.TypeKindStruct, Name: "decodeAsUser", ClassName: "decodeAsUser"}
	id, err := set.RegisterAuto("decodeAsUser", desc)
	if err != nil {
		t.Fatalf("RegisterAuto: %v", err)
	}
	c := NewWithResolver(&transport.JSONCodec{}, set)

	got, err := DecodeAs[decodeAsUser](c, id, message.EncodingJSON, []byte(`{"Name":"ada","Age":36}`))
	if err != nil {
		t.Fatalf("DecodeAs: %v", err)
	}
	if got.Name != "ada" || got.Age != 36 {
		t.Fatalf("decoded %+v, want {Name:ada Age:36}", got)
	}
}

func TestCodec_DecodeAs_UnknownSchema(t *testing.T) {
	c := NewJSON()
	got, err := DecodeAs[decodeAsUser](c, 9999, message.EncodingJSON, []byte(`{}`))
	if err == nil {
		t.Fatalf("expected error for unknown schema ID, got %+v", got)
	}
}

func TestCodec_DecodeAs_UntypedPayloadRejected(t *testing.T) {
	c := NewJSON()
	got, err := DecodeAs[decodeAsUser](c, schema.BuiltinMapStringString, message.EncodingNone, []byte(`{}`))
	if err == nil {
		t.Fatalf("expected error for EncodingNone, got %+v", got)
	}
}
