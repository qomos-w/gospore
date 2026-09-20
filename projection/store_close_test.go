package projection

import (
	"testing"
	"time"

	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/spore/binding"
)

func mustAID(t *testing.T) id.ActorID {
	t.Helper()
	aid, err := id.Parse("01000000000000000000000000000000")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return aid
}

// TestStoreCloseStopsBatchTimers pins the shutdown contract: Close tears
// down every watch subscription (Recv reports EOF) including ones with a
// pending batch timer, and is idempotent — the AfterFunc would otherwise
// fire after shutdown.
func TestStoreCloseStopsBatchTimers(t *testing.T) {
	s := NewStore(8)
	aid := mustAID(t)

	sub, err := s.Watch(aid, WithBatchTimeout(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	// Prime a pending delta so a batch timer is armed.
	s.Publish(aid, Snapshot{Version: 1, Fields: map[string]binding.ViewProjection{
		"f": {Fields: map[string]any{"v": 1}},
	}})

	s.Close()
	s.Close() // idempotent

	if _, err := sub.Recv(); err == nil {
		t.Fatal("Recv after store Close succeeded, want EOF")
	}
	// The armed timer must not resurrect anything: after its nominal
	// deadline the subscription stays terminal and nothing panics.
	time.Sleep(90 * time.Millisecond)
	if _, err := sub.Recv(); err == nil {
		t.Fatal("Recv after timer deadline post-Close succeeded, want EOF")
	}
}
