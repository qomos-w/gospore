package transport

import (
	"testing"

	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/message"
)

// Transport Send baselines: Local is the synchronous direct-delivery
// path (one map lookup + one Recv call), MemoryRemote is the paired
// channel hop used for same-process cross-app tests. These pin the
// floor any routing optimization must beat.

func benchFrame(i int) message.Frame {
	return message.Frame{
		Kind:   message.KindCall,
		To:     benchTo,
		From:   benchFrom,
		CorID:  uint64(i),
		CallID: "bench.call",
	}
}

var benchTo = parseBenchID("01000000000000000000000000000001")
var benchFrom = parseBenchID("02000000000000000000000000000002")

func parseBenchID(s string) id.ActorID {
	v, err := id.Parse(s)
	if err != nil {
		panic(err)
	}
	return v
}

func BenchmarkLocalSend(b *testing.B) {
	t := NewLocal()
	t.Register(benchTo, func(env mailbox.Envelope) error { return nil })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := t.Send(benchFrame(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMemoryRemoteSend(b *testing.B) {
	a, bb := NewMemoryRemotePair()
	// Drain the receiving side so Sends measure the buffered hop. The
	// channel is bounded and lossy under burst (Send is non-blocking),
	// so the benchmark retries on buffer-full and reports the per-frame
	// cost of successful hops — the drain goroutine keeps pace over
	// time, matching steady-state cross-app usage.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case _, ok := <-bb.Receive():
				if !ok {
					return
				}
			case <-stop:
				return
			}
		}
	}()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for {
			err := a.Send(benchFrame(i))
			if err == nil {
				break
			}
			if err.Error() == "transport: remote outbound buffer full" {
				continue
			}
			b.Fatal(err)
		}
	}
	b.StopTimer()
	close(stop)
}
