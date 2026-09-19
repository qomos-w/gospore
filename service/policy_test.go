package service

import (
	"errors"
	"testing"
)

func TestScopedRegistrationError(t *testing.T) {
	if err := ScopedRegistrationError(false, false); err != nil {
		t.Fatalf("clean registration = %v, want nil", err)
	}
	if err := ScopedRegistrationError(true, false); !errors.Is(err, ErrScopedConflict) {
		t.Fatalf("globalTaken = %v, want ErrScopedConflict", err)
	}
	if err := ScopedRegistrationError(false, true); !errors.Is(err, ErrScopedConflict) {
		t.Fatalf("ownerTaken = %v, want ErrScopedConflict", err)
	}
}

func TestGlobalRegistrationError(t *testing.T) {
	cases := []struct {
		name                            string
		self, other, scoped             bool
		want                            error
	}{
		{"clean", false, false, false, nil},
		{"idempotent self", true, false, false, nil},
		{"self precedence", true, true, true, nil},
		{"other owner", false, true, false, ErrServiceNameTaken},
		{"scoped conflict", false, false, true, ErrScopedConflict},
	}
	for _, tc := range cases {
		got := GlobalRegistrationError(tc.self, tc.other, tc.scoped)
		if !errors.Is(got, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}
