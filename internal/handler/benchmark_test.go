package handler

import (
	"reflect"
	"testing"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/codec"
	"github.com/qomos-w/gospore/message"
	tschema "github.com/qomos-w/spore/schema"
)

// BenchmarkDispatch_Throughput measures handler table lookup + Run closure
// execution for a simple stateful handler (Context, string) → (string, error).
func BenchmarkDispatch_Throughput(b *testing.B) {
	tbl := NewTable()
	fn := func(_ actor.Context, req string) (string, error) {
		return req, nil
	}
	if err := tbl.Register("test.echo", fn); err != nil {
		b.Fatalf("Register: %v", err)
	}

	inv, ok := tbl.Lookup("test.echo")
	if !ok {
		b.Fatal("Lookup failed")
	}

	env := InvokeEnv{
		Frame:   message.Frame{CallID: "test.echo", Body: []byte(`"hello"`)},
		Context: nil, // context injection is Cell-tier; benchmark focuses on handler dispatch
		Payload: []byte(`"hello"`),
		Reply:   func(_ message.Frame) {},
		Codec:   codec.NewJSON(),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		inv.Run(env)
	}
}

// BenchmarkDispatch_WithEmitter measures streaming handler dispatch where
// an Emitter parameter is injected.
func BenchmarkDispatch_WithEmitter(b *testing.B) {
	tbl := NewTable()
	fn := func(_ actor.Context, em actor.Emitter) error {
		_ = em.Send("chunk")
		return nil
	}
	if err := tbl.Register("test.stream", fn); err != nil {
		b.Fatalf("Register: %v", err)
	}

	inv, ok := tbl.Lookup("test.stream")
	if !ok {
		b.Fatal("Lookup failed")
	}

	env := InvokeEnv{
		Frame:   message.Frame{CallID: "test.stream"},
		Context: nil,
		Reply:   func(_ message.Frame) {},
		Codec:   codec.NewJSON(),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		inv.Run(env)
	}
}

// BenchmarkDispatch_Stateless measures stateless (PureContext) handler
// dispatch. The reflective pipeline is identical; the difference is only
// in which goroutine runs it.
func BenchmarkDispatch_Stateless(b *testing.B) {
	tbl := NewTable()
	fn := func(_ actor.PureContext, req []byte) ([]byte, error) {
		return req, nil
	}
	if err := tbl.Register("test.pure", fn); err != nil {
		b.Fatalf("Register: %v", err)
	}

	inv, ok := tbl.Lookup("test.pure")
	if !ok {
		b.Fatal("Lookup failed")
	}

	env := InvokeEnv{
		Frame:   message.Frame{CallID: "test.pure", Body: []byte("payload")},
		Context: nil,
		Payload: []byte("payload"),
		Reply:   func(_ message.Frame) {},
		Codec:   codec.NewJSON(),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		inv.Run(env)
	}
}

// BenchmarkBuildArgs measures just the reflect arg construction in isolation.
func BenchmarkBuildArgs(b *testing.B) {
	fn := func(_ actor.Context, req string) (string, error) {
		return req, nil
	}
	fv := reflect.ValueOf(fn)
	fnType := fv.Type()
	mode := actor.ModeStateful
	env := InvokeEnv{
		Frame:   message.Frame{Body: []byte(`"hello"`)},
		Context: nil,
		Payload: []byte(`"hello"`),
		Reply:   func(_ message.Frame) {},
		Codec:   codec.NewJSON(),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = buildArgs(fnType, mode, env, tschema.TypeDesc{}, 0, tschema.TypeDesc{}, message.EncodingNone)
	}
}
