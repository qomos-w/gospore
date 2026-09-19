package resource

import (
	"testing"
)

func TestNew(t *testing.T) {
	r := New()
	if r == nil {
		t.Fatal("New() returned nil")
	}
	if r.Frozen() {
		t.Error("new registry should not be frozen")
	}
}

func TestSetAndGet(t *testing.T) {
	r := New()
	k := NewKey[string]("db")

	if err := Set(r, k, "postgres"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	v, ok := Get(r, k)
	if !ok {
		t.Fatal("Get returned false")
	}
	if v != "postgres" {
		t.Errorf("Get = %q, want \"postgres\"", v)
	}
}

func TestGet_Miss(t *testing.T) {
	r := New()
	k := NewKey[int]("missing")
	v, ok := Get(r, k)
	if ok {
		t.Error("Get on missing key should return false")
	}
	if v != 0 {
		t.Errorf("Get zero value = %d, want 0", v)
	}
}

func TestGet_TypeMismatch(t *testing.T) {
	r := New()
	k := NewKey[string]("config")
	r.Set(k, int64(42)) // wrong type stored via raw interface

	v, ok := Get(r, k)
	if ok {
		t.Error("Get with type mismatch should return false")
	}
	if v != "" {
		t.Errorf("Get zero value = %q, want \"\"", v)
	}
}

func TestMustGet(t *testing.T) {
	r := New()
	k := NewKey[string]("db")
	Set(r, k, "postgres")

	if v := MustGet(r, k); v != "postgres" {
		t.Errorf("MustGet = %q, want \"postgres\"", v)
	}
}

func TestMustGet_MissPanics(t *testing.T) {
	r := New()
	k := NewKey[string]("missing")

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("MustGet on missing key should panic")
		}
	}()
	MustGet(r, k)
}

func TestMustGet_TypeMismatchPanics(t *testing.T) {
	r := New()
	k := NewKey[string]("config")
	r.Set(k, int64(42))

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("MustGet with type mismatch should panic")
		}
	}()
	MustGet(r, k)
}

func TestHas(t *testing.T) {
	r := New()
	k := NewKey[int]("counter")
	if r.Has(k) {
		t.Error("Has on empty registry should be false")
	}
	Set(r, k, 7)
	if !r.Has(k) {
		t.Error("Has after Set should be true")
	}
}

func TestFreeze(t *testing.T) {
	r := New()
	k := NewKey[string]("db")
	Set(r, k, "postgres")

	Freeze(r)
	if !r.Frozen() {
		t.Error("registry should be frozen after Freeze")
	}

	if err := Set(r, k, "mysql"); err != ErrFrozen {
		t.Fatalf("Set after freeze: got %v, want ErrFrozen", err)
	}
}

func TestFreeze_Idempotent(t *testing.T) {
	r := New()
	Freeze(r)
	Freeze(r) // should not panic
	if !r.Frozen() {
		t.Error("registry should still be frozen")
	}
}

func TestKey_Equality(t *testing.T) {
	k1 := NewKey[string]("x")
	k2 := NewKey[string]("x")

	// Same type, same name — struct value comparison is true
	if k1 != k2 {
		t.Error("keys with same type and name should compare equal")
	}
}

func TestDiagConstants(t *testing.T) {
	if DiagUnknown != "gospore.resource.unknown" {
		t.Errorf("DiagUnknown = %q", DiagUnknown)
	}
	if DiagTypeMismatch != "gospore.resource.type_mismatch" {
		t.Errorf("DiagTypeMismatch = %q", DiagTypeMismatch)
	}
}
