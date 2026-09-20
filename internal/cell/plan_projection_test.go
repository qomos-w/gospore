package cell

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/projection"
	"github.com/qomos-w/gospore/ref"
)

// --- minimal doubles for plan node projection tests ---

type pruneStream struct {
	chunks []any
	idx    int
	mu     sync.Mutex
}

func (s *pruneStream) Recv() (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.idx < len(s.chunks) {
		v := s.chunks[s.idx]
		s.idx++
		return v, nil
	}
	return nil, io.EOF
}

func (s *pruneStream) RecvRaw() ([]byte, error) { return nil, io.EOF }
func (s *pruneStream) Close() error             { return nil }

type pruneTarget struct{ stream *pruneStream }

func (m *pruneTarget) ID() id.ActorID                                   { return id.ActorID{} }
func (m *pruneTarget) Service() (string, bool)                          { return "", false }
func (m *pruneTarget) Invoke(context.Context, string, any, ...map[string]string) *invoke.Call {
	return invoke.NewCall(invoke.CallModeStream, m.stream)
}

type pruneSelfRef struct{ aid id.ActorID }

func (r pruneSelfRef) ID() id.ActorID          { return r.aid }
func (r pruneSelfRef) Service() (string, bool) { return "", false }
func (r pruneSelfRef) Invoke(context.Context, string, any, ...map[string]string) *invoke.Call {
	return nil
}

var _ ref.Ref = (*pruneTarget)(nil)
var _ ref.Ref = pruneSelfRef{}

func pruneTestID(slot uint16) id.ActorID {
	return id.NewCanonical(slot, 0, func() uint64 { return 1 }).Next()
}

// TestPlanNode_OnStop_PrunesProjection confirms OnStop removes the
// ephemeral plan node's projection entry so completed invokes do not
// accumulate snapshots in the store for the process lifetime.
func TestPlanNode_OnStop_PrunesProjection(t *testing.T) {
	store := projection.NewStore(0)
	self := pruneSelfRef{aid: pruneTestID(11)}
	target := &pruneTarget{stream: &pruneStream{chunks: []any{"hello"}}}
	node := NewPlanNodeWithRefForTest(target, "test.greet", "world", self, store)

	if err := node.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	<-node.Done()

	if _, ok := store.Get(self.ID()); !ok {
		t.Fatal("pre-stop: projection snapshot missing")
	}

	pn, ok := node.(*planNodeActor)
	if !ok {
		t.Fatalf("node is %T, want *planNodeActor", node)
	}
	if err := pn.OnStop(nil); err != nil {
		t.Fatalf("OnStop: %v", err)
	}

	if _, ok := store.Get(self.ID()); ok {
		t.Fatal("post-stop: projection snapshot still present")
	}
}

// TestPlanNode_Projection_KeptAfterDone confirms the completed projection
// stays published until teardown (pruning only happens in OnStop).
func TestPlanNode_Projection_KeptAfterDone(t *testing.T) {
	store := projection.NewStore(0)
	self := pruneSelfRef{aid: pruneTestID(12)}
	target := &pruneTarget{stream: &pruneStream{chunks: []any{"x"}}}
	node := NewPlanNodeWithRefForTest(target, "test.fast", nil, self, store)

	if err := node.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	<-node.Done()

	if _, ok := store.Get(self.ID()); !ok {
		t.Fatal("after done but before stop: projection snapshot missing")
	}
}
