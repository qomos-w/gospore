package service

import (
	"context"
	"testing"

	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/spore/identity"
)

// mockRef is a minimal ref.Ref for testing.
type mockRef struct {
	actorID id.ActorID
}

func (m *mockRef) ID() id.ActorID                                    { return m.actorID }
func (m *mockRef) Service() (string, bool)                           { return "", false }
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

func TestValidName(t *testing.T) {
	valid := []string{"api", "auth", "user_service", "v1-api"}
	for _, n := range valid {
		if !ValidName(n) {
			t.Errorf("ValidName(%q) should be true", n)
		}
	}

	invalid := []string{"", "Api", "user.service", "123", "user service"}
	for _, n := range invalid {
		if ValidName(n) {
			t.Errorf("ValidName(%q) should be false", n)
		}
	}
}

func TestRegistry_RegisterAndLookup(t *testing.T) {
	r := New()
	ref1 := newMockRef(0x01)

	if err := r.Register("api", ref1); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got, ok := r.Lookup("api")
	if !ok {
		t.Fatal("Lookup should succeed")
	}
	if got.ID() != ref1.ID() {
		t.Error("Lookup returned wrong ref")
	}
}

func TestRegistry_Register_InvalidName(t *testing.T) {
	r := New()
	ref1 := newMockRef(0x01)

	if err := r.Register("Bad.Name", ref1); err != ErrInvalidServiceName {
		t.Fatalf("Register invalid: got %v, want ErrInvalidServiceName", err)
	}
}

func TestRegistry_Register_Duplicate(t *testing.T) {
	r := New()
	ref1 := newMockRef(0x01)
	ref2 := newMockRef(0x02)

	r.Register("api", ref1)
	if err := r.Register("api", ref2); err != ErrServiceNameTaken {
		t.Fatalf("duplicate Register: got %v, want ErrServiceNameTaken", err)
	}
}

func TestRegistry_Unregister(t *testing.T) {
	r := New()
	ref1 := newMockRef(0x01)

	r.Register("api", ref1)
	r.Unregister("api")

	if _, ok := r.Lookup("api"); ok {
		t.Error("Lookup after Unregister should fail")
	}
}

func TestRegistry_Unregister_NotFound(t *testing.T) {
	r := New()
	// unregistering a non-existent name should not panic
	r.Unregister("missing")
}

func TestRegistry_Names(t *testing.T) {
	r := New()
	ref1 := newMockRef(0x01)

	r.Register("z", ref1)
	r.Register("a", ref1)
	r.Register("m", ref1)

	names := r.Names()
	if len(names) != 3 {
		t.Fatalf("len(Names) = %d, want 3", len(names))
	}
	// should be sorted
	if names[0] != "a" || names[1] != "m" || names[2] != "z" {
		t.Errorf("Names not sorted: %v", names)
	}
}

func TestRegistry_Lookup_Miss(t *testing.T) {
	r := New()
	if _, ok := r.Lookup("missing"); ok {
		t.Error("Lookup on empty registry should fail")
	}
}
