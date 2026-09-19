package tree

import (
	"context"
	"testing"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/ref"
	"github.com/qomos-w/spore/identity"
)

// mockRef is a minimal ref.Ref for testing Tree.
type mockRef struct {
	actorID id.ActorID
}

func (m *mockRef) ID() id.ActorID                              { return m.actorID }

func (m *mockRef) Service() (string, bool)                     { return "", false }
func (m *mockRef) Invoke(_ context.Context, _ string, _ any, _ ...map[string]string) *invoke.Call {
	return nil
}

func newMockRef(raw byte) *mockRef {
	var buf [16]byte
	buf[0] = raw
	return &mockRef{
		actorID: id.From(identity.CanonicalID(buf)),
	}
}

func testConfig() Config {
	root := newMockRef(0x01)
	return Config{
		Root: root,
		Allocator: &AllocatorFuncs{
			ReserveFn: func(parent ref.Ref, props actor.Props, name string) (ref.Ref, error) {
				r := rawCounter
				rawCounter++
				return newMockRef(r), nil
			},
			BuildFn: func(parent ref.Ref, props actor.Props, name string, reserved ref.Ref) error {
				return nil
			},
		},
		Idler:      func(r ref.Ref) error { return nil },
		Terminator: func(r ref.Ref) error { return nil },
	}
}

var rawCounter byte = 0x10

func resetCounter() { rawCounter = 0x10 }


func TestTree_New_MissingConfig(t *testing.T) {
	_, err := New(Config{})
	if err == nil {
		t.Fatal("New with empty Config should error")
	}
}

func TestTree_New_OK(t *testing.T) {
	resetCounter()
	cfg := testConfig()
	tr, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if tr.Root() == nil {
		t.Fatal("Root should not be nil")
	}
}

func TestTree_Spawn(t *testing.T) {
	resetCounter()
	cfg := testConfig()
	tr, _ := New(cfg)

	ref, err := tr.Spawn(tr.Root(), actor.Props{}, "child-a")
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if ref == nil {
		t.Fatal("Spawn returned nil ref")
	}
}

func TestTree_Spawn_NameTaken(t *testing.T) {
	resetCounter()
	cfg := testConfig()
	tr, _ := New(cfg)

	tr.Spawn(tr.Root(), actor.Props{}, "dup")
	_, err := tr.Spawn(tr.Root(), actor.Props{}, "dup")
	if err != ErrNameTaken {
		t.Fatalf("duplicate Spawn: got %v, want ErrNameTaken", err)
	}
}

func TestTree_Spawn_UnknownParent(t *testing.T) {
	resetCounter()
	cfg := testConfig()
	tr, _ := New(cfg)

	orphan := newMockRef(0xFF)
	_, err := tr.Spawn(orphan, actor.Props{}, "x")
	if err != ErrUnknownParent {
		t.Fatalf("Spawn with unknown parent: got %v, want ErrUnknownParent", err)
	}
}

func TestTree_Spawn_NilParent(t *testing.T) {
	resetCounter()
	cfg := testConfig()
	tr, _ := New(cfg)

	_, err := tr.Spawn(nil, actor.Props{}, "x")
	if err != ErrUnknownParent {
		t.Fatalf("Spawn with nil parent: got %v, want ErrUnknownParent", err)
	}
}



func TestTree_LookupID(t *testing.T) {
	resetCounter()
	cfg := testConfig()
	tr, _ := New(cfg)

	child, _ := tr.Spawn(tr.Root(), actor.Props{}, "a")

	if _, ok := tr.LookupID(child.ID()); !ok {
		t.Error("LookupID(child) should succeed")
	}
	if _, ok := tr.LookupID(id.ActorID{}); ok {
		t.Error("LookupID(zero) should fail")
	}
}

func TestTree_Children(t *testing.T) {
	resetCounter()
	cfg := testConfig()
	tr, _ := New(cfg)

	tr.Spawn(tr.Root(), actor.Props{}, "a")
	tr.Spawn(tr.Root(), actor.Props{}, "b")

	kids := tr.Children(tr.Root())
	if len(kids) != 2 {
		t.Fatalf("len(Children) = %d, want 2", len(kids))
	}
}

func TestTree_Children_NilParent(t *testing.T) {
	resetCounter()
	cfg := testConfig()
	tr, _ := New(cfg)

	if kids := tr.Children(nil); kids != nil {
		t.Errorf("Children(nil) = %v, want nil", kids)
	}
}

func TestTree_Parent(t *testing.T) {
	resetCounter()
	cfg := testConfig()
	tr, _ := New(cfg)

	child, _ := tr.Spawn(tr.Root(), actor.Props{}, "a")
	parent, ok := tr.Parent(child)
	if !ok {
		t.Fatal("Parent should succeed")
	}
	if parent.ID() != tr.Root().ID() {
		t.Error("Parent should be root")
	}
}

func TestTree_Parent_Root(t *testing.T) {
	resetCounter()
	cfg := testConfig()
	tr, _ := New(cfg)

	_, ok := tr.Parent(tr.Root())
	if ok {
		t.Error("root should have no parent")
	}
}

func TestTree_Parent_Nil(t *testing.T) {
	resetCounter()
	cfg := testConfig()
	tr, _ := New(cfg)

	_, ok := tr.Parent(nil)
	if ok {
		t.Error("Parent(nil) should be false")
	}
}

func TestTree_Stop(t *testing.T) {
	resetCounter()
	var idled []string
	cfg := testConfig()
	cfg.Idler = func(r ref.Ref) error {
		idled = append(idled, r.ID().String())
		return nil
	}
	tr, _ := New(cfg)

	child, _ := tr.Spawn(tr.Root(), actor.Props{}, "a")
	if err := tr.Stop(child); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if _, ok := tr.LookupID(child.ID()); !ok {
		t.Error("stopped child should remain in tree")
	}
	if len(idled) != 1 || idled[0] != child.ID().String() {
		t.Errorf("idled = %v, want child ID", idled)
	}
}

func TestTree_Stop_UnknownTarget(t *testing.T) {
	resetCounter()
	cfg := testConfig()
	tr, _ := New(cfg)

	orphan := newMockRef(0xFF)
	if err := tr.Stop(orphan); err != ErrUnknownTarget {
		t.Fatalf("Stop unknown: got %v, want ErrUnknownTarget", err)
	}
	if err := tr.Destroy(orphan); err != ErrUnknownTarget {
		t.Fatalf("Destroy unknown: got %v, want ErrUnknownTarget", err)
	}
}

func TestTree_Stop_NilTarget(t *testing.T) {
	resetCounter()
	cfg := testConfig()
	tr, _ := New(cfg)

	if err := tr.Stop(nil); err != ErrUnknownTarget {
		t.Fatalf("Stop nil: got %v, want ErrUnknownTarget", err)
	}
	if err := tr.Destroy(nil); err != ErrUnknownTarget {
		t.Fatalf("Destroy nil: got %v, want ErrUnknownTarget", err)
	}
}

func TestTree_Destroy_LIFO(t *testing.T) {
	resetCounter()
	var order []string
	cfg := testConfig()
	cfg.Terminator = func(r ref.Ref) error {
		order = append(order, r.ID().String())
		return nil
	}
	tr, _ := New(cfg)

	parent, _ := tr.Spawn(tr.Root(), actor.Props{}, "p")
	tr.Spawn(parent, actor.Props{}, "c1")
	tr.Spawn(parent, actor.Props{}, "c2")

	tr.Destroy(parent)

	// LIFO: descendants first, then target
	if len(order) != 3 {
		t.Fatalf("terminated %d actors, want 3", len(order))
	}
	if order[2] != parent.ID().String() {
		t.Errorf("last terminated = %q, want \"/p\"", order[2])
	}
}

func TestTree_Walk(t *testing.T) {
	resetCounter()
	cfg := testConfig()
	tr, _ := New(cfg)

	tr.Spawn(tr.Root(), actor.Props{}, "a")
	tr.Spawn(tr.Root(), actor.Props{}, "b")

	var count int
	tr.Walk(func(r ref.Ref) bool {
		count++
		return true
	})
	if count != 3 { // root + a + b
		t.Errorf("Walk visited %d nodes, want 3", count)
	}
}

func TestTree_Walk_EarlyExit(t *testing.T) {
	resetCounter()
	cfg := testConfig()
	tr, _ := New(cfg)

	tr.Spawn(tr.Root(), actor.Props{}, "a")
	tr.Spawn(tr.Root(), actor.Props{}, "b")

	var count int
	tr.Walk(func(r ref.Ref) bool {
		count++
		return false // stop after first
	})
	if count != 1 {
		t.Errorf("Walk visited %d nodes, want 1", count)
	}
}

func TestTree_Walk_NilVisitor(t *testing.T) {
	resetCounter()
	cfg := testConfig()
	tr, _ := New(cfg)

	tr.Walk(nil) // should not panic
}

func TestTree_DiagConstants(t *testing.T) {
	if DiagCycle != "gospore.tree.cycle" {
		t.Errorf("DiagCycle = %q", DiagCycle)
	}
	if DiagNameTaken != "gospore.tree.name_taken" {
		t.Errorf("DiagNameTaken = %q", DiagNameTaken)
	}
	if DiagNotChild != "gospore.tree.not_child" {
		t.Errorf("DiagNotChild = %q", DiagNotChild)
	}
}

func TestTree_MapErrToDiag(t *testing.T) {
	if got := MapErrToDiag(nil); got != "" {
		t.Errorf("nil → %q", got)
	}
	if got := MapErrToDiag(ErrCycle); got != DiagCycle {
		t.Errorf("ErrCycle → %q, want %q", got, DiagCycle)
	}
	if got := MapErrToDiag(ErrNameTaken); got != DiagNameTaken {
		t.Errorf("ErrNameTaken → %q, want %q", got, DiagNameTaken)
	}
	if got := MapErrToDiag(ErrUnknownParent); got != "" {
		t.Errorf("ErrUnknownParent → %q, want \"\"", got)
	}
}
