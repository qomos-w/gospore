package schema

import (
	"errors"
	"testing"

	spore "github.com/qomos-w/spore/schema"
)

func TestDiagConstants(t *testing.T) {
	cases := []struct{ got, want string }{
		{DiagIDZero, "gospore.schema.id_zero"},
		{DiagIDTaken, "gospore.schema.id_taken"},
		{DiagNameTaken, "gospore.schema.name_taken"},
		{DiagImportConflict, "gospore.schema.import_conflict"},
		{DiagUnknown, "gospore.schema.unknown"},
		{DiagUnregisteredForCallable, "gospore.schema.unregistered_for_callable"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("got %q, want %q", c.got, c.want)
		}
	}
}

func TestSentinelErrors(t *testing.T) {
	if ErrSchemaIDZero == nil {
		t.Error("ErrSchemaIDZero should not be nil")
	}
	if ErrSchemaIDTaken == nil {
		t.Error("ErrSchemaIDTaken should not be nil")
	}
	if ErrSchemaNameTaken == nil {
		t.Error("ErrSchemaNameTaken should not be nil")
	}
	if ErrSchemaImportConflict == nil {
		t.Error("ErrSchemaImportConflict should not be nil")
	}
	if ErrSchemaInvalidNamespace == nil {
		t.Error("ErrSchemaInvalidNamespace should not be nil")
	}
	if ErrSchemaReadOnly == nil {
		t.Error("ErrSchemaReadOnly should not be nil")
	}
}

func TestMapErrToDiag(t *testing.T) {
	if got := MapErrToDiag(nil); got != "" {
		t.Errorf("nil → %q, want \"\"", got)
	}
	if got := MapErrToDiag(ErrSchemaIDZero); got != DiagIDZero {
		t.Errorf("ErrSchemaIDZero → %q, want %q", got, DiagIDZero)
	}
	if got := MapErrToDiag(ErrSchemaIDTaken); got != DiagIDTaken {
		t.Errorf("ErrSchemaIDTaken → %q, want %q", got, DiagIDTaken)
	}
	if got := MapErrToDiag(ErrSchemaNameTaken); got != DiagNameTaken {
		t.Errorf("ErrSchemaNameTaken → %q, want %q", got, DiagNameTaken)
	}
	if got := MapErrToDiag(ErrSchemaImportConflict); got != DiagImportConflict {
		t.Errorf("ErrSchemaImportConflict → %q, want %q", got, DiagImportConflict)
	}
	if got := MapErrToDiag(ErrSchemaInvalidNamespace); got != "" {
		// ErrSchemaInvalidNamespace intentionally returns "" per diag.go
		t.Errorf("ErrSchemaInvalidNamespace → %q, want \"\"", got)
	}
	if got := MapErrToDiag(errors.New("other")); got != "" {
		t.Errorf("other → %q, want \"\"", got)
	}
}

func TestNew(t *testing.T) {
	s, err := New("test")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s == nil {
		t.Fatal("New() returned nil")
	}
	if s.Namespace() != "test" {
		t.Errorf("Namespace = %q, want \"test\"", s.Namespace())
	}
}

func TestNew_InvalidNamespace(t *testing.T) {
	_, err := New("Bad.Namespace")
	if err == nil {
		t.Error("New with invalid namespace should error")
	}
}

func TestSet_RegisterAndLookup(t *testing.T) {
	s, _ := New("test")

	err := s.Register(1, "User", spore.TypeDesc{Name: "User"}, spore.ObjectDesc{})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	ent, ok := s.Lookup(1)
	if !ok {
		t.Fatal("Lookup should succeed")
	}
	if ent.Name != "User" {
		t.Errorf("Name = %q, want \"User\"", ent.Name)
	}
}

func TestSet_Register_ZeroID(t *testing.T) {
	s, _ := New("test")
	if err := s.Register(0, "X", spore.TypeDesc{}, spore.ObjectDesc{}); !errors.Is(err, ErrSchemaIDZero) {
		t.Fatalf("Register(0): got %v, want ErrSchemaIDZero", err)
	}
}

func TestSet_Register_DuplicateID(t *testing.T) {
	s, _ := New("test")
	s.Register(1, "A", spore.TypeDesc{Name: "A"}, spore.ObjectDesc{})
	if err := s.Register(1, "B", spore.TypeDesc{Name: "B"}, spore.ObjectDesc{}); !errors.Is(err, ErrSchemaIDTaken) {
		t.Fatalf("duplicate ID: got %v, want ErrSchemaIDTaken", err)
	}
}

func TestSet_Register_DuplicateName(t *testing.T) {
	s, _ := New("test")
	s.Register(1, "A", spore.TypeDesc{Name: "A"}, spore.ObjectDesc{})
	if err := s.Register(2, "A", spore.TypeDesc{Name: "B"}, spore.ObjectDesc{}); !errors.Is(err, ErrSchemaNameTaken) {
		t.Fatalf("duplicate Name: got %v, want ErrSchemaNameTaken", err)
	}
}

func TestSet_Lookup_Miss(t *testing.T) {
	s, _ := New("test")
	if _, ok := s.Lookup(99); ok {
		t.Error("Lookup on empty set should fail")
	}
}

func TestSet_LookupByName(t *testing.T) {
	s, _ := New("test")
	s.Register(1, "User", spore.TypeDesc{Name: "User"}, spore.ObjectDesc{})

	ent, ok := s.LookupByName("User")
	if !ok {
		t.Fatal("LookupByName should succeed")
	}
	if ent.ID != 1 {
		t.Errorf("ID = %d, want 1", ent.ID)
	}
}

func TestSet_Resolve(t *testing.T) {
	s, _ := New("test")
	s.Register(1, "User", spore.TypeDesc{Name: "User"}, spore.ObjectDesc{})

	ent, ok := s.Resolve("test", 1)
	if !ok {
		t.Fatal("Resolve should succeed")
	}
	if ent.Name != "User" {
		t.Errorf("Name = %q, want \"User\"", ent.Name)
	}

	if _, ok := s.Resolve("other", 1); ok {
		t.Error("Resolve with wrong namespace should fail")
	}
}

func TestSet_Namespaces(t *testing.T) {
	s, _ := New("test")
	ns := s.Namespaces()
	if len(ns) != 1 || ns[0] != "test" {
		t.Errorf("Namespaces = %v, want [test]", ns)
	}
}

func TestSet_ReadOnly(t *testing.T) {
	data := []byte(`{"ns":"test","entries":[]}`)
	s, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if err := s.Register(1, "X", spore.TypeDesc{}, spore.ObjectDesc{}); !errors.Is(err, ErrSchemaReadOnly) {
		t.Fatalf("Register on read-only: got %v, want ErrSchemaReadOnly", err)
	}
}

func TestSet_MarshalRoundTrip(t *testing.T) {
	s, _ := New("test")
	s.Register(1, "User", spore.TypeDesc{Name: "User", Kind: spore.TypeKindScalar}, spore.ObjectDesc{})

	data, err := s.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	s2, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	ent, ok := s2.Lookup(1)
	if !ok {
		t.Fatal("Lookup after round-trip should succeed")
	}
	if ent.Name != "User" {
		t.Errorf("Name = %q, want \"User\"", ent.Name)
	}
}
