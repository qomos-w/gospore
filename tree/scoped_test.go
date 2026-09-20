package tree

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/ref"
	"github.com/qomos-w/gospore/service"
)

type fakeRef struct {
	id id.ActorID
}

func (r *fakeRef) ID() id.ActorID                       { return r.id }
func (r *fakeRef) Service() (string, bool)              { return "", false }
func (r *fakeRef) Invoke(context.Context, string, any, ...map[string]string) *invoke.Call {
	return nil
}

var _ ref.Ref = (*fakeRef)(nil)

func mustID(t *testing.T, s string) id.ActorID {
	t.Helper()
	 aid, err := id.Parse(s)
	if err != nil {
		t.Fatalf("parse id %q: %v", s, err)
	}
	return aid
}

func childIDForSeq(n uint64) string {
	return fmt.Sprintf("0000000000000000%016x", n)
}

func newTestTree(t *testing.T, rootID string) (Tree, *fakeRef) {
	t.Helper()
	root := &fakeRef{id: mustID(t, rootID)}
	var seq uint64
	tr, err := New(Config{
		Root: root,
		Allocator: &AllocatorFuncs{
			ReserveFn: func(parent ref.Ref, props actor.Props, name string) (ref.Ref, error) {
				seq++
				return &fakeRef{id: mustID(t, childIDForSeq(seq))}, nil
			},
			BuildFn: func(parent ref.Ref, props actor.Props, name string, reserved ref.Ref) error {
				return nil
			},
		},
		Idler:      func(ref.Ref) error { return nil },
		Terminator: func(ref.Ref) error { return nil },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return tr, root
}

const rootIDHex = "00000000000000010000000000000001"

func TestScopedServiceLookup(t *testing.T) {
	tr, root := newTestTree(t, rootIDHex)
	child, err := tr.Spawn(root, actor.PropsFromFunc(func() actor.Actor { return nil }), "child")
	if err != nil {
		t.Fatalf("Spawn child: %v", err)
	}
	grandchild, err := tr.Spawn(child, actor.PropsFromFunc(func() actor.Actor { return nil }), "grandchild")
	if err != nil {
		t.Fatalf("Spawn grandchild: %v", err)
	}

	if err := tr.RegisterScopedService(child, "svc", child); err != nil {
		t.Fatalf("RegisterScopedService: %v", err)
	}

	if r, ok := tr.LookupScopedService(grandchild, "svc"); !ok || r.ID() != child.ID() {
		t.Fatalf("grandchild should resolve to child, got %v %v", r, ok)
	}
	if _, ok := tr.LookupScopedService(child, "svc"); ok {
		t.Fatalf("owner should not resolve to its own scoped service")
	}
	if _, ok := tr.LookupScopedService(root, "svc"); ok {
		t.Fatalf("ancestor should not resolve to descendant scoped service")
	}
	if _, ok := tr.LookupScopedService(grandchild, "missing"); ok {
		t.Fatalf("missing service should not resolve")
	}
}

func TestScopedServiceNearestAncestor(t *testing.T) {
	tr, root := newTestTree(t, rootIDHex)
	child, _ := tr.Spawn(root, actor.PropsFromFunc(func() actor.Actor { return nil }), "child")
	grandchild, _ := tr.Spawn(child, actor.PropsFromFunc(func() actor.Actor { return nil }), "grandchild")
	great, _ := tr.Spawn(grandchild, actor.PropsFromFunc(func() actor.Actor { return nil }), "great")

	if err := tr.RegisterScopedService(child, "svc", child); err != nil {
		t.Fatalf("register child scoped: %v", err)
	}
	if err := tr.RegisterScopedService(grandchild, "svc", grandchild); err != nil {
		t.Fatalf("register grandchild scoped: %v", err)
	}

	if r, ok := tr.LookupScopedService(great, "svc"); !ok || r.ID() != grandchild.ID() {
		t.Fatalf("great should resolve to nearest ancestor grandchild, got %v", r)
	}
	if r, ok := tr.LookupScopedService(grandchild, "svc"); !ok || r.ID() != child.ID() {
		t.Fatalf("grandchild should resolve to nearest ancestor child, got %v", r)
	}
}

func TestScopedServiceGlobalConflict(t *testing.T) {
	tr, root := newTestTree(t, rootIDHex)
	child, _ := tr.Spawn(root, actor.PropsFromFunc(func() actor.Actor { return nil }), "child")

	if err := tr.RegisterGlobalService(root, "svc", root); err != nil {
		t.Fatalf("register root global: %v", err)
	}
	if err := tr.RegisterScopedService(child, "svc", child); !errors.Is(err, service.ErrScopedConflict) {
		t.Fatalf("scoped under global should conflict, got %v", err)
	}
}

func TestGlobalServiceScopedConflict(t *testing.T) {
	tr, root := newTestTree(t, rootIDHex)
	child, _ := tr.Spawn(root, actor.PropsFromFunc(func() actor.Actor { return nil }), "child")

	if err := tr.RegisterScopedService(root, "svc", root); err != nil {
		t.Fatalf("register root scoped: %v", err)
	}
	if err := tr.RegisterGlobalService(child, "svc", child); !errors.Is(err, service.ErrScopedConflict) {
		t.Fatalf("global under scoped should conflict, got %v", err)
	}
}

func TestScopedServiceCleanupOnDestroy(t *testing.T) {
	tr, root := newTestTree(t, rootIDHex)
	child, _ := tr.Spawn(root, actor.PropsFromFunc(func() actor.Actor { return nil }), "child")

	if err := tr.RegisterScopedService(child, "svc", child); err != nil {
		t.Fatalf("register scoped: %v", err)
	}
	if err := tr.Destroy(child); err != nil {
		t.Fatalf("Destroy child: %v", err)
	}
	if err := tr.RegisterScopedService(child, "svc", child); !errors.Is(err, ErrUnknownTarget) {
		t.Fatalf("register after destroy should fail with unknown target, got %v", err)
	}
}
