package invoke

import (
	"context"
	"testing"

	"github.com/qomos-w/gospore/message"
)

// BenchmarkPendingTable_Deliver measures the hot-path frame routing
// from callee back to caller via the pending table.
func BenchmarkPendingTable_Deliver(b *testing.B) {
	pt := NewPendingTable()
	ch := make(chan message.Frame, 1)
	pt.Register(1, "", "", ch)
	frame := message.Frame{CorID: 1, Kind: message.KindReply, Body: []byte("ok")}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// Drain the previous send so the channel never blocks.
		select {
		case <-ch:
		default:
		}
		_ = pt.Deliver(frame)
	}
}

// BenchmarkPendingTable_RegisterUnregister measures the lifecycle
// overhead of a single call slot.
func BenchmarkPendingTable_RegisterUnregister(b *testing.B) {
	pt := NewPendingTable()
	ch := make(chan message.Frame, 1)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pt.Register(uint64(i), "", "", ch)
		pt.Unregister(uint64(i))
	}
}

// BenchmarkCall_Once measures lazy unary consumption via value().
func BenchmarkCall_Once(b *testing.B) {
	ch := make(chan message.Frame, 4)
	ch <- message.Frame{Kind: message.KindReply, Body: []byte("hello")}
	ch <- message.Frame{Kind: message.KindEnd}
	close(ch)
	stream := NewStream(ch, 1, NewPendingTable(), nil)
	call := NewCall(CallModeUnary, stream)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// value memoises; benchmark the first call only.
		_ = call.value()
	}
}

// BenchmarkCall_Next measures streaming chunk consumption.
func BenchmarkCall_Next(b *testing.B) {
	const batch = 1000
	ch := make(chan message.Frame, batch+1)
	for i := 0; i < batch; i++ {
		ch <- message.Frame{Kind: message.KindReply, Body: []byte("chunk")}
	}
	ch <- message.Frame{Kind: message.KindEnd}
	close(ch)
	stream := NewStream(ch, 1, NewPendingTable(), nil)
	call := NewCall(CallModeStream, stream)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = call.Next(ctx)
	}
}
