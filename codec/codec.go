// Package codec is gospore's narrow value↔bytes adapter on top of spore.
//
// codec deliberately holds NO naming or ID registry — that's schema.Set's
// job. Every encode / decode call carries a spore.TypeDesc explicitly:
//
//	schemaSet.Resolve(ns, id) → Entry.Desc → codec.Encode/Decode(desc, ...)
//
// This split lets gospore add new naming / addressing schemes by extending
// schema.Set without touching codec.
package codec

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/qomos-w/gospore/message"
	"github.com/qomos-w/gospore/schema"
	"github.com/qomos-w/spore/identity"
	sporesch "github.com/qomos-w/spore/schema"
	"github.com/qomos-w/spore/transport"
)

// tbcMagic is the Spore Binary Codec header.
var tbcMagic = []byte{0x54, 0x42, 0x43, 0x02} // "TBC\x02"

// IsTBCData reports whether data begins with the TBC binary codec magic.
func IsTBCData(data []byte) bool {
	return len(data) >= len(tbcMagic) && bytes.Equal(data[:len(tbcMagic)], tbcMagic)
}

// Codec encodes and decodes spore.TypeDesc-annotated values to and
// from wire bytes.
type Codec interface {
	Encode(desc sporesch.TypeDesc, value any) ([]byte, error)
	Decode(desc sporesch.TypeDesc, data []byte) (any, error)
	Encoding() message.Encoding

	// DecodeByID resolves schemaID to a TypeDesc via the codec's
	// SchemaResolver and decodes data accordingly. EncodingNone means the
	// payload is already the value itself and is returned unchanged.
	DecodeByID(schemaID uint64, encoding message.Encoding, data []byte) (any, error)

	// DecodeByIDInto resolves schemaID and decodes wire data directly into
	// target using reflection. The target must be a non-nil pointer.
	// This avoids the generic map[string]any intermediate form for struct
	// types, populating Go struct fields directly from the binary stream.
	DecodeByIDInto(schemaID uint64, encoding message.Encoding, data []byte, target any) error
}

// EncodingAwareCodec extends Codec with the ability to encode using a
// specific wire encoding rather than the codec's default.
type EncodingAwareCodec interface {
	Codec
	// EncodeAs encodes value using the specified encoding. For single-codec
	// wrappers encoding must match Encoding(); multi-codec wrappers dispatch
	// to the appropriate backend.
	EncodeAs(desc sporesch.TypeDesc, value any, encoding message.Encoding) ([]byte, error)
}

// SchemaResolver looks up the TypeDesc that a global schema ID points to.
// schema.Set satisfies this implicitly via its LookupSchema method; codec
// keeps the interface narrow to avoid an import cycle with schema.
type SchemaResolver interface {
	LookupSchema(id uint64) (sporesch.TypeDesc, bool)
	LookupObject(id uint64) (sporesch.ObjectDesc, bool)
}

// DecodeAs resolves schemaID and decodes data into a fresh T, wrapping
// DecodeByIDInto:
//
//	var out T
//	err := c.DecodeByIDInto(schemaID, encoding, data, &out)
//
// It is a package-level function because Codec is an interface and
// interface methods cannot declare type parameters.
func DecodeAs[T any](c Codec, schemaID uint64, encoding message.Encoding, data []byte) (T, error) {
	var out T
	err := c.DecodeByIDInto(schemaID, encoding, data, &out)
	return out, err
}

// New wraps a spore transport.Codec backend in the gospore Codec contract.
// A nil backend is a programming error and panics. The returned codec has
// no SchemaResolver — DecodeByID only resolves builtin IDs.
func New(backend transport.Codec) Codec {
	if backend == nil {
		panic("gospore/codec: nil transport.Codec backend")
	}
	return &wrapper{backend: backend}
}

// NewWithResolver wraps backend and binds a SchemaResolver so that
// DecodeByID can materialise typed Go values from wire bytes alone.
// A nil resolver is allowed and equivalent to New.
func NewWithResolver(backend transport.Codec, resolver SchemaResolver) Codec {
	if backend == nil {
		panic("gospore/codec: nil transport.Codec backend")
	}
	return &wrapper{backend: backend, resolver: resolver}
}

// NewJSON returns a Codec wrapping spore's JSONCodec.
func NewJSON() Codec {
	return &wrapper{backend: &transport.JSONCodec{}}
}

// NewBinary returns a Codec wrapping spore's BinaryCodec.
func NewBinary() Codec {
	return &wrapper{backend: &transport.BinaryCodec{}}
}

// NewMulti creates a Codec that supports both JSON and Binary wire
// encodings. Decode/DecodeByID dispatch to the appropriate backend by
// inspecting the encoding parameter or the TBC magic header. Encode
// defaults to JSON; use EncodeAs (via the EncodingAwareCodec interface)
// to encode in a specific format.
func NewMulti(resolver SchemaResolver) Codec {
	return &multiWrapper{
		json:   &wrapper{backend: &transport.JSONCodec{}, resolver: resolver},
		binary: &wrapper{backend: &transport.BinaryCodec{}, resolver: resolver},
	}
}

// wrapper adapts a spore transport.Codec to the gospore Codec interface.
// gospore's contract is bytes-in / bytes-out; spore carries a richer
// View with Identity and Kind metadata, which we fill with stable defaults.
type wrapper struct {
	backend  transport.Codec
	resolver SchemaResolver
}

func (w *wrapper) Encoding() message.Encoding {
	switch w.backend.(type) {
	case *transport.JSONCodec:
		return message.EncodingJSON
	case *transport.BinaryCodec:
		return message.EncodingBinary
	default:
		return message.EncodingNone
	}
}

// Encode forwards to the spore backend; if value is already []byte,
// it is returned unchanged so Value-mode byte-slice payloads can bypass
// a codec round-trip.
func (w *wrapper) Encode(desc sporesch.TypeDesc, value any) ([]byte, error) {
	if b, ok := value.([]byte); ok {
		return b, nil
	}
	view, err := w.backend.Encode(desc, identity.CanonicalID{}, value)
	if err != nil {
		return nil, err
	}
	return view.Data, nil
}

// Decode reconstructs a Go value from data using the spore backend.
// Identity is left zero; gospore.codec is identity-agnostic.
func (w *wrapper) Decode(desc sporesch.TypeDesc, data []byte) (any, error) {
	view := transport.View{
		Kind:   transport.ViewKindFull,
		Schema: desc,
		Data:   data,
	}
	return w.backend.Decode(view)
}

// DecodeByID resolves schemaID and decodes data according to the declared
// frame encoding. EncodingNone means the caller is transporting the value
// itself rather than codec-serialised bytes. BuiltinAny means the contract
// is explicitly untyped — data is returned as raw bytes for the caller to
// interpret, regardless of declared encoding.
func (w *wrapper) DecodeByID(schemaID uint64, encoding message.Encoding, data []byte) (any, error) {
	if encoding == message.EncodingNone {
		return data, nil
	}
	if schemaID == schema.BuiltinAny {
		return data, nil
	}
	desc, ok := schema.BuiltinDesc(schemaID)
	if !ok && w.resolver != nil {
		desc, ok = w.resolver.LookupSchema(schemaID)
	}
	if !ok {
		// Unknown schema (e.g. cross-app reply where caller hasn't imported
		// the server's manifest). Return raw bytes for the caller to interpret.
		return data, nil
	}
	if encoding != w.Encoding() {
		return nil, fmt.Errorf("gospore/codec: encoding mismatch have %s want %s", w.Encoding(), encoding)
	}
	return w.Decode(desc, data)
}

func (w *wrapper) DecodeByIDInto(schemaID uint64, encoding message.Encoding, data []byte, target any) error {
	if encoding == message.EncodingNone || schemaID == schema.BuiltinAny {
		return fmt.Errorf("gospore/codec: DecodeByIDInto not applicable for untyped payload")
	}
	desc, ok := schema.BuiltinDesc(schemaID)
	if !ok && w.resolver != nil {
		desc, ok = w.resolver.LookupSchema(schemaID)
	}
	if !ok {
		return fmt.Errorf("gospore/codec: unknown schema ID %d for DecodeByIDInto", schemaID)
	}
	if encoding != message.EncodingNone && encoding != w.Encoding() {
		return fmt.Errorf("gospore/codec: encoding mismatch have %s want %s", w.Encoding(), encoding)
	}
	if into, ok := w.backend.(transport.IntoDecoder); ok {
		view := transport.View{
			Kind:   transport.ViewKindFull,
			Schema: desc,
			Data:   data,
		}
		return into.DecodeInto(view, target)
	}
	// Fallback: decode then assign via reflection.
	val, err := w.DecodeByID(schemaID, encoding, data)
	if err != nil {
		return err
	}
	return assignValue(reflect.ValueOf(target).Elem(), val)
}

// EncodeAs implements EncodingAwareCodec for single-backend wrappers.
// The requested encoding must match the wrapper's own Encoding().
func (w *wrapper) EncodeAs(desc sporesch.TypeDesc, value any, encoding message.Encoding) ([]byte, error) {
	if encoding != w.Encoding() {
		return nil, fmt.Errorf("gospore/codec: EncodeAs encoding mismatch have %s want %s", w.Encoding(), encoding)
	}
	return w.Encode(desc, value)
}

// multiWrapper holds both JSON and Binary backends and dispatches
// decode/encode operations by encoding. It satisfies both Codec and
// EncodingAwareCodec.
type multiWrapper struct {
	json   *wrapper
	binary *wrapper
}

func (m *multiWrapper) Encoding() message.Encoding { return message.EncodingJSON }

func (m *multiWrapper) Encode(desc sporesch.TypeDesc, value any) ([]byte, error) {
	return m.json.Encode(desc, value)
}

func (m *multiWrapper) Decode(desc sporesch.TypeDesc, data []byte) (any, error) {
	if IsTBCData(data) {
		return m.binary.Decode(desc, data)
	}
	return m.json.Decode(desc, data)
}

func (m *multiWrapper) DecodeByID(schemaID uint64, encoding message.Encoding, data []byte) (any, error) {
	if encoding == message.EncodingNone || schemaID == schema.BuiltinAny {
		return data, nil
	}
	switch encoding {
	case message.EncodingBinary:
		return m.binary.DecodeByID(schemaID, encoding, data)
	case message.EncodingJSON:
		return m.json.DecodeByID(schemaID, encoding, data)
	default:
		if IsTBCData(data) {
			return m.binary.DecodeByID(schemaID, encoding, data)
		}
		return m.json.DecodeByID(schemaID, encoding, data)
	}
}

func (m *multiWrapper) DecodeByIDInto(schemaID uint64, encoding message.Encoding, data []byte, target any) error {
	if encoding == message.EncodingNone || schemaID == schema.BuiltinAny {
		return fmt.Errorf("gospore/codec: DecodeByIDInto not applicable for untyped payload")
	}
	switch encoding {
	case message.EncodingBinary:
		return m.binary.DecodeByIDInto(schemaID, encoding, data, target)
	case message.EncodingJSON:
		return m.json.DecodeByIDInto(schemaID, encoding, data, target)
	default:
		if IsTBCData(data) {
			return m.binary.DecodeByIDInto(schemaID, encoding, data, target)
		}
		return m.json.DecodeByIDInto(schemaID, encoding, data, target)
	}
}

// EncodeAs implements EncodingAwareCodec for multi-backend wrappers.
func (m *multiWrapper) EncodeAs(desc sporesch.TypeDesc, value any, encoding message.Encoding) ([]byte, error) {
	switch encoding {
	case message.EncodingBinary:
		return m.binary.Encode(desc, value)
	default:
		return m.json.Encode(desc, value)
	}
}

// assignValue populates dst reflect.Value from a decoded src value.
// Used as a fallback when the transport backend does not implement IntoDecoder.
func assignValue(dst reflect.Value, src any) error {
	if src == nil {
		return nil
	}
	sv := reflect.ValueOf(src)
	if sv.Type().AssignableTo(dst.Type()) {
		dst.Set(sv)
		return nil
	}
	switch dst.Kind() {
	case reflect.Struct:
		m, ok := src.(map[string]any)
		if !ok {
			return fmt.Errorf("expected map for struct %s, got %T", dst.Type().Name(), src)
		}
		for key, val := range m {
			field := dst.FieldByName(key)
			if !field.IsValid() || !field.CanSet() {
				continue
			}
			if err := assignValue(field, val); err != nil {
				return fmt.Errorf(".%s: %w", key, err)
			}
		}
		return nil
	case reflect.Slice:
		if dst.Type().Elem().Kind() == reflect.Uint8 {
			if b, ok := src.([]byte); ok {
				dst.SetBytes(b)
				return nil
			}
		}
		arr, ok := src.([]any)
		if !ok {
			return fmt.Errorf("expected array for slice %s, got %T", dst.Type().Name(), src)
		}
		slice := reflect.MakeSlice(dst.Type(), len(arr), len(arr))
		for i, elem := range arr {
			if err := assignValue(slice.Index(i), elem); err != nil {
				return fmt.Errorf("[%d]: %w", i, err)
			}
		}
		dst.Set(slice)
		return nil
	case reflect.Map:
		m, ok := src.(map[string]any)
		if !ok {
			return fmt.Errorf("expected map for map %s, got %T", dst.Type().Name(), src)
		}
		result := reflect.MakeMapWithSize(dst.Type(), len(m))
		for key, val := range m {
			valVal := reflect.New(dst.Type().Elem()).Elem()
			if err := assignValue(valVal, val); err != nil {
				return fmt.Errorf("[%s]: %w", key, err)
			}
			result.SetMapIndex(reflect.ValueOf(key), valVal)
		}
		dst.Set(result)
		return nil
	case reflect.Pointer:
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		return assignValue(dst.Elem(), src)
	default:
		if sv.Type().ConvertibleTo(dst.Type()) {
			dst.Set(sv.Convert(dst.Type()))
			return nil
		}
		return fmt.Errorf("cannot assign %T to %s", src, dst.Type())
	}
}
