package id

import (
	"testing"

	sporeIdentity "github.com/qomos-w/spore/identity"
)

func TestActorID_Zero(t *testing.T) {
	var a ActorID
	if !a.IsZero() {
		t.Error("zero ActorID should be IsZero")
	}
}

func TestActorID_CanonicalRoundTrip(t *testing.T) {
	var raw [16]byte
	raw[0] = 0xAB
	cid := sporeIdentity.CanonicalID(raw)
	a := From(cid)
	if a.Canonical() != cid {
		t.Error("Canonical round-trip failed")
	}
}

func TestCorIDGenerator(t *testing.T) {
	g := NewCorIDGenerator()
	if g.Next() != 1 {
		t.Errorf("first CorID = %d, want 1", g.Next()-1)
	}
	if g.Next() != 2 {
		t.Errorf("second CorID = %d, want 2", g.Next()-1)
	}
	if g.Next() != 3 {
		t.Errorf("third CorID = %d, want 3", g.Next()-1)
	}
}

func TestCanonical_Next(t *testing.T) {
	var ts uint64 = 123456789
	g := NewCanonical(1, 2, func() uint64 { return ts })

	a1 := g.Next()
	a2 := g.Next()

	if a1.IsZero() {
		t.Error("first ID should not be zero")
	}
	if a1 == a2 {
		t.Fatal("consecutive IDs should be unique")
	}
	if a1.TimestampMS() != ts {
		t.Errorf("timestamp = %d, want %d", a1.TimestampMS(), ts)
	}
	if a1.RuntimeSlot() != 1 {
		t.Errorf("slot = %d, want 1", a1.RuntimeSlot())
	}
	if a1.Incarnation() != 2 {
		t.Errorf("incarnation = %d, want 2", a1.Incarnation())
	}
}

func TestCanonical_SequenceOrdering(t *testing.T) {
	g := NewCanonical(0, 0, func() uint64 { return 1000 })

	a1 := g.Next()
	a2 := g.Next()

	if a1.Sequence() >= a2.Sequence() {
		t.Errorf("sequence not monotonic: %d >= %d", a1.Sequence(), a2.Sequence())
	}
}

func TestIdentity_IsZero(t *testing.T) {
	var zero Identity
	if !zero.IsZero() {
		t.Error("zero Identity should be IsZero")
	}
	if (Identity{Kind: IdentityToken}).IsZero() {
		t.Error("Identity with Token kind should not be IsZero")
	}
	if (Identity{Subject: "x"}).IsZero() {
		t.Error("Identity with Subject should not be IsZero")
	}
	if (Identity{Role: "admin"}).IsZero() {
		t.Error("Identity with Role should not be IsZero")
	}
}

func TestIdentityKind_String(t *testing.T) {
	cases := []struct {
		k    IdentityKind
		want string
	}{
		{IdentityAnonymous, "anonymous"},
		{IdentityToken, "token"},
		{IdentityAppCertificate, "app_certificate"},
		{IdentityKind(99), ""},
	}
	for _, c := range cases {
		if got := c.k.String(); got != c.want {
			t.Errorf("IdentityKind(%d).String() = %q, want %q", c.k, got, c.want)
		}
	}
}

func TestRole_Effective(t *testing.T) {
	cases := []struct {
		role Role
		want []Role
	}{
		{"admin", []Role{"admin"}},
		{"agent.admin", []Role{"agent.admin", "agent"}},
		{"a.b.c", []Role{"a.b.c", "a.b", "a"}},
	}
	for _, c := range cases {
		got := c.role.Effective()
		if len(got) != len(c.want) {
			t.Fatalf("Effective(%q) len = %d, want %d", c.role, len(got), len(c.want))
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("Effective(%q)[%d] = %q, want %q", c.role, i, got[i], c.want[i])
			}
		}
	}
}

func TestValidRoleName(t *testing.T) {
	valid := []string{"admin", "agent", "agent_admin", "agent.admin", "a.b.c"}
	for _, s := range valid {
		if !ValidRoleName(s) {
			t.Errorf("ValidRoleName(%q) should be true", s)
		}
	}

	invalid := []string{"", "Admin", "agent.", ".agent", "agent..admin", "agent-admin"}
	for _, s := range invalid {
		if ValidRoleName(s) {
			t.Errorf("ValidRoleName(%q) should be false", s)
		}
	}
}
