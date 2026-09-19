package projection

import (
	"testing"

	"github.com/qomos-w/gospore/actor"
)

type actorWithVis struct {
	PublicField     string `gospore:"component,public"`
	AdminField      int    `gospore:"component,admin"`
	DiagnosticField bool   `gospore:"component,diagnostic"`
	InternalField   string `gospore:"component,internal"`
	DefaultField    int    `gospore:"component"`
	SkipMe          string `json:"name"`
}

func TestScanComponents_Visibility(t *testing.T) {
	a := &actorWithVis{}
	slots, err := ScanComponents(a)
	if err != nil {
		t.Fatalf("ScanComponents: %v", err)
	}
	if len(slots) != 5 {
		t.Fatalf("len(slots) = %d, want 5", len(slots))
	}

	want := []struct {
		name string
		vis  actor.Visibility
	}{
		{"PublicField", actor.VisibilityPublic},
		{"AdminField", actor.VisibilityAdmin},
		{"DiagnosticField", actor.VisibilityDiagnostic},
		{"InternalField", actor.VisibilityInternal},
		{"DefaultField", actor.VisibilityInternal},
	}

	for i, w := range want {
		if slots[i].Name != w.name {
			t.Errorf("slots[%d].Name = %q, want %q", i, slots[i].Name, w.name)
		}
		if slots[i].Visibility != w.vis {
			t.Errorf("slots[%d].Visibility = %v, want %v", i, slots[i].Visibility, w.vis)
		}
	}
}

func TestScanComponents_InvalidVisibility(t *testing.T) {
	type badActor struct {
		BadField string `gospore:"component,secret"`
	}
	_, err := ScanComponents(&badActor{})
	if err == nil {
		t.Fatal("expected error for invalid visibility token")
	}
}

func TestScanComponents_TooManyTokens(t *testing.T) {
	type badActor struct {
		BadField string `gospore:"component,public,extra"`
	}
	_, err := ScanComponents(&badActor{})
	if err == nil {
		t.Fatal("expected error for too many tag tokens")
	}
}
