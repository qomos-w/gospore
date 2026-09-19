package actor

import "testing"

func TestVisibilityString(t *testing.T) {
	cases := []struct {
		v    Visibility
		want string
	}{
		{VisibilityInternal, "internal"},
		{VisibilityDiagnostic, "diagnostic"},
		{VisibilityAdmin, "admin"},
		{VisibilityPublic, "public"},
		{Visibility(99), ""},
	}
	for _, c := range cases {
		if got := c.v.String(); got != c.want {
			t.Errorf("Visibility(%d).String() = %q, want %q", c.v, got, c.want)
		}
	}
}

func TestParseVisibility(t *testing.T) {
	cases := []struct {
		input   string
		wantVis Visibility
		wantOk  bool
	}{
		{"internal", VisibilityInternal, true},
		{"diagnostic", VisibilityDiagnostic, true},
		{"admin", VisibilityAdmin, true},
		{"public", VisibilityPublic, true},
		{"PUBLIC", VisibilityInternal, false},
		{"", VisibilityInternal, false},
		{"unknown", VisibilityInternal, false},
	}
	for _, c := range cases {
		gotVis, gotOk := ParseVisibility(c.input)
		if gotVis != c.wantVis || gotOk != c.wantOk {
			t.Errorf("ParseVisibility(%q) = (%v, %v), want (%v, %v)",
				c.input, gotVis, gotOk, c.wantVis, c.wantOk)
		}
	}
}

func TestVisibilityRoundTrip(t *testing.T) {
	for v := VisibilityInternal; v <= VisibilityPublic; v++ {
		s := v.String()
		parsed, ok := ParseVisibility(s)
		if !ok {
			t.Errorf("ParseVisibility(%q) failed for Visibility(%d)", s, v)
		}
		if parsed != v {
			t.Errorf("round-trip failed: Visibility(%d) -> %q -> Visibility(%d)", v, s, parsed)
		}
	}
}

func TestVisibilityOrdering(t *testing.T) {
	// Internal < Diagnostic < Admin < Public
	if VisibilityInternal >= VisibilityDiagnostic {
		t.Error("VisibilityInternal should be < VisibilityDiagnostic")
	}
	if VisibilityDiagnostic >= VisibilityAdmin {
		t.Error("VisibilityDiagnostic should be < VisibilityAdmin")
	}
	if VisibilityAdmin >= VisibilityPublic {
		t.Error("VisibilityAdmin should be < VisibilityPublic")
	}
}
