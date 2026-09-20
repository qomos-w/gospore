// Package wiregen renders the TypeScript artifacts that must stay in
// lockstep with gospore's Go wire definitions:
//
//   - web-client/src/generated/system_protocol.ts
//     (system-reserved schema IDs, TypeDescs, ObjectDescs — source of
//     truth: schema/builtin.go)
//   - web-client/testdata/frame_vector.json
//     (golden marshal/decode vectors for the GSF wire frame — source of
//     truth: gateway/frame_binary.go)
//
// sync_test.go compares the committed artifacts against the renderers
// and rewrites them when run with -update. The TypeScript test suite
// consumes the same frame vector, so Go/TS format drift fails tests on
// either side.
package wiregen

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/qomos-w/gospore/gateway"
	"github.com/qomos-w/gospore/schema"
)

// systemProtocolIDs lists the system-reserved struct schema IDs in
// ascending order. The IDs themselves live in schema/builtin.go; this
// list fixes the render order of the generated module.
var systemProtocolIDs = []uint64{
	schema.BuiltinAuthReq,
	schema.BuiltinAuthOk,
	schema.BuiltinEventSubscribeServiceReq,
	schema.BuiltinEventSubscribeInstanceReq,
	schema.BuiltinProjectionGetReq,
	schema.BuiltinProjectionWatchReq,
}

// tsConstName maps a builtin ID to the exported constant name used by
// the generated module (same names the hand-written session.ts block
// used, so callers keep their identifiers).
func tsConstName(id uint64) string {
	switch id {
	case schema.BuiltinAuthReq:
		return "SYS_AUTH_REQ"
	case schema.BuiltinAuthOk:
		return "SYS_AUTH_OK"
	case schema.BuiltinEventSubscribeServiceReq:
		return "SYS_EVENT_SUBSCRIBE_SERVICE_REQ"
	case schema.BuiltinEventSubscribeInstanceReq:
		return "SYS_EVENT_SUBSCRIBE_INSTANCE_REQ"
	case schema.BuiltinProjectionGetReq:
		return "SYS_PROJECTION_GET_REQ"
	case schema.BuiltinProjectionWatchReq:
		return "SYS_PROJECTION_WATCH_REQ"
	}
	panic(fmt.Sprintf("wiregen: no TS name for builtin id %d", id))
}

// tsIdent converts a system struct or field name (e.g.
// "projectionGetReq", "ServiceName") to SCREAMING_SNAKE (e.g.
// "PROJECTION_GET_REQ", "SERVICE_NAME").
func tsIdent(name string) string {
	var b strings.Builder
	for i, r := range name {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				b.WriteByte('_')
			}
			b.WriteRune(r)
		} else {
			b.WriteRune(r)
		}
	}
	s := strings.ToUpper(b.String())
	return s
}

// RenderSystemProtocolTS renders web-client/src/generated/system_protocol.ts.
func RenderSystemProtocolTS() string {
	var b strings.Builder
	b.WriteString("// AUTO-GENERATED from gospore schema/builtin.go — DO NOT EDIT.\n")
	b.WriteString("// Regenerate: go test ./internal/wiregen -update\n")
	b.WriteString("import type { TypeDesc, ObjectDesc } from \"../schema.js\";\n\n")

	for _, id := range systemProtocolIDs {
		fmt.Fprintf(&b, "export const %s = %d;\n", tsConstName(id), id)
	}
	b.WriteString("\n")

	b.WriteString("const scalarCache: Record<string, TypeDesc> = {};\n")
	b.WriteString("function scalar(name: string): TypeDesc {\n")
	b.WriteString("  return (scalarCache[name] ??= { kind: \"scalar\", name });\n")
	b.WriteString("}\n\n")

	for _, id := range systemProtocolIDs {
		desc, _ := schema.BuiltinDesc(id)
		obj, _ := schema.BuiltinObject(id)
		ident := tsIdent(desc.Name)
		fmt.Fprintf(&b, "export const %s_DESC: TypeDesc = { kind: \"struct\", name: %q, className: %q, classId: %s };\n",
			ident, desc.Name, desc.ClassName, tsConstName(id))
		var fields []string
		for _, f := range obj.Fields {
			fmt.Fprintf(&b, "export const %s_FIELD_%s: TypeDesc = scalar(%q);\n",
				ident, tsIdent(f.Name), f.Type.Name)
			fields = append(fields, fmt.Sprintf("{ name: %q, type: %s_FIELD_%s }", f.Name, ident, tsIdent(f.Name)))
		}
		fmt.Fprintf(&b, "export const %s_OBJECT: ObjectDesc = { kind: \"struct\", name: %q, fields: [%s] };\n\n",
			ident, obj.Name, strings.Join(fields, ", "))
	}

	b.WriteString("// The two scalars the system protocol uses directly.\n")
	b.WriteString("export const STRING_TYPE: TypeDesc = scalar(\"string\");\n")
	b.WriteString("export const UINT64_TYPE: TypeDesc = scalar(\"uint64\");\n")
	return b.String()
}

// TSFrame is the JSON projection of gateway.WireFrame used by both the
// Go renderer and the TypeScript vector test. u64 fields are decimal
// strings so JSON and JS numbers lose no precision; bytes are hex.
type TSFrame struct {
	Flags    uint8       `json:"flags"`
	Type     uint8       `json:"type"`
	CorID    string      `json:"corId"`
	Seq      uint32      `json:"seq"`
	TransID  string      `json:"transId"`
	CallID   string      `json:"callId"`
	SubID    string      `json:"subId"`
	Target   string      `json:"target"`
	From     string      `json:"from"`
	ErrorMsg string      `json:"errorMsg"`
	Payload  *string     `json:"payload"`  // hex; null = empty payload
	Headers  [][2]string `json:"headers"`  // ordered pairs; both sides re-sort on marshal
}

// FrameCase is one golden vector: a logical frame plus its canonical
// wire encoding produced by the real gateway.MarshalWireFrame.
type FrameCase struct {
	Name  string  `json:"name"`
	Frame TSFrame `json:"frame"`
	Wire  string  `json:"wire"`
}

// ErrorCase is a malformed or truncated wire sample both
// implementations must reject.
type ErrorCase struct {
	Name  string `json:"name"`
	Wire  string `json:"wire"`
	Error string `json:"error"` // "truncated" | "malformed"
}

type frameVector struct {
	Version int         `json:"version"`
	Format  string      `json:"format"`
	Cases   []FrameCase `json:"cases"`
	Errors  []ErrorCase `json:"errors"`
}

func hexOf(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return hex.EncodeToString(b)
}

func ptrHex(b []byte) *string {
	if len(b) == 0 {
		return nil
	}
	s := hex.EncodeToString(b)
	return &s
}

func u64s(v uint64) string { return strconv.FormatUint(v, 10) }

func mkFrame(name string, f gateway.WireFrame) FrameCase {
	wire, err := gateway.MarshalWireFrame(&f)
	if err != nil {
		panic(fmt.Sprintf("wiregen: marshal %s: %v", name, err))
	}
	var hdrs [][2]string
	for k, v := range f.Headers {
		hdrs = append(hdrs, [2]string{k, v})
	}
	sort.Slice(hdrs, func(i, j int) bool { return hdrs[i][0] < hdrs[j][0] })
	return FrameCase{
		Name: name,
		Frame: TSFrame{
			Flags:    uint8(f.Flags),
			Type:     uint8(f.Type),
			CorID:    u64s(f.CorID),
			Seq:      f.Seq,
			TransID:  u64s(f.TransID),
			CallID:   f.CallID,
			SubID:    f.SubID,
			Target:   f.Target,
			From:     f.From,
			ErrorMsg: f.ErrorMsg,
			Payload:  ptrHex(f.Payload),
			Headers:  hdrs,
		},
		Wire: hexOf(wire),
	}
}

// frameCases is the golden matrix, marshaled by the real
// gateway.MarshalWireFrame — never hand-encoded.
func frameCases() []FrameCase {
	return []FrameCase{
		mkFrame("invoke_minimal", gateway.WireFrame{
			Flags:  gateway.MakeFlags(gateway.EncodingJSON, gateway.CompressionNone),
			Type:   gateway.FrameTypeInvoke,
			CorID:  1,
			CallID: "test.echo",
		}),
		mkFrame("reply_with_payload", gateway.WireFrame{
			Flags:  gateway.MakeFlags(gateway.EncodingJSON, gateway.CompressionNone),
			Type:   gateway.FrameTypeReply,
			CorID:  300,
			CallID: "test.echo",
			Payload: []byte(`{"ok":true}`),
		}),
		mkFrame("error_frame", gateway.WireFrame{
			Flags:    gateway.MakeFlags(gateway.EncodingJSON, gateway.CompressionNone),
			Type:     gateway.FrameTypeError,
			CorID:    7,
			CallID:   "diag.slow",
			ErrorMsg: "gospore.timeout: deadline exceeded",
		}),
		mkFrame("binary_encoded_chunk", gateway.WireFrame{
			Flags:  gateway.MakeFlags(gateway.EncodingBinary, gateway.CompressionNone),
			Type:   gateway.FrameTypeChunk,
			CorID:  2,
			Seq:    42,
			CallID: "test.stream",
		}),
		mkFrame("subscribe_unicode", gateway.WireFrame{
			Flags:   gateway.MakeFlags(gateway.EncodingJSON, gateway.CompressionNone),
			Type:    gateway.FrameTypeSubscribe,
			CorID:   4294967296,
			SubID:   "sub-日本語",
			Target:  "workspace.agent",
		}),
		mkFrame("uvarint_edges", gateway.WireFrame{
			Flags:   gateway.MakeFlags(gateway.EncodingBinary, gateway.CompressionGzip),
			Type:    gateway.FrameTypeEnd,
			CorID:   18446744073709551615,
			Seq:     4294967295,
			TransID: 18446744073709551615,
			CallID:  "x",
		}),
		// "é" sorts after "z" byte-wise (0xC3 > 0x7A) but before it under
		// most locale collations — pins byte-wise header ordering.
		mkFrame("headers_bytesort_pins_locale", gateway.WireFrame{
			Flags:  gateway.MakeFlags(gateway.EncodingJSON, gateway.CompressionNone),
			Type:   gateway.FrameTypeInvoke,
			CorID:  128,
			CallID: "h",
			Headers: map[string]string{
				"z-key": "1", "é-key": "2", "a-key": "3",
			},
		}),
		mkFrame("from_trailing", gateway.WireFrame{
			Flags:  gateway.MakeFlags(gateway.EncodingJSON, gateway.CompressionNone),
			Type:   gateway.FrameTypeReply,
			CorID:  9,
			CallID: "c",
			From:   "actor-0001",
		}),
		mkFrame("ping_minimal", gateway.WireFrame{
			Flags: gateway.MakeFlags(gateway.EncodingJSON, gateway.CompressionNone),
			Type:  gateway.FrameTypePing,
		}),
		mkFrame("empty_payload_explicit", gateway.WireFrame{
			Flags: gateway.MakeFlags(gateway.EncodingJSON, gateway.CompressionNone),
			Type:  gateway.FrameTypeReply,
			CorID: 5,
		}),
	}
}

// RenderFrameVectorJSON renders web-client/testdata/frame_vector.json.
func RenderFrameVectorJSON() ([]byte, error) {
	cases := frameCases()
	sort.Slice(cases, func(i, j int) bool { return cases[i].Name < cases[j].Name })
	v := frameVector{
		Version: 2,
		Format:  "GSF v2: magic 47534601, version byte 02, flags, type, corId u64be, seq u32be, transId u64be, uvarint strings callId/subId/target/errorMsg, uvarint payload, sorted headers, trailing from",
		Cases:   cases,
		Errors:  errorCases(),
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func errorCases() []ErrorCase {
	return []ErrorCase{
		{Name: "bad_magic", Wire: hexOf([]byte{'X', 'S', 'F', 0x01, 0x02}), Error: "malformed"},
		{Name: "bad_version", Wire: hexOf([]byte{'G', 'S', 'F', 0x01, 0x03}), Error: "malformed"},
		{Name: "truncated_fixed", Wire: hexOf([]byte{'G', 'S', 'F', 0x01, 0x02, 0x00, 0x01, 0x00}), Error: "truncated"},
		{Name: "truncated_varint_string", Wire: hexOf([]byte{
			'G', 'S', 'F', 0x01, 0x02, 0x00, 0x01,
			0, 0, 0, 0, 0, 0, 0, 0, // corId
			0, 0, 0, 0, // seq
			0, 0, 0, 0, 0, 0, 0, 0, // transId
			5, 'a', // callId length 5 but only 1 byte follows
		}), Error: "truncated"},
	}
}
