package events

import (
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
)

// Ring Push baseline: the hot path every actor's event write goes
// through. The wraparound case (ring at capacity, every Push evicts)
// is the steady-state shape for chatty actors.

func benchRecord(i int) Record {
	return Record{
		SeqNo:     uint64(i),
		Kind:      actor.WatchStarted,
		Timestamp: time.Now(),
		CallID:    "bench.call",
		SchemaNS:  "bench",
		SchemaID:  7,
		CorID:     uint64(i),
		Duration:  time.Millisecond,
	}
}

func BenchmarkRingPush(b *testing.B) {
	b.Run("default_256", func(b *testing.B) {
		r := NewRing(256)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r.Push(benchRecord(i))
		}
	})
	b.Run("large_4096", func(b *testing.B) {
		r := NewRing(4096)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r.Push(benchRecord(i))
		}
	})
}

func BenchmarkStorePush(b *testing.B) {
	// Store-level Push includes the per-actor ring lookup plus the
	// RemoteSink snapshot path exercised on every event write.
	s := NewStore(256)
	actorID, err := id.Parse("01000000000000000000000000000001")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Push(actorID, benchRecord(i))
	}
}
