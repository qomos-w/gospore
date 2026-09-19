package actor

import "testing"

// TestBase_Lifecycle confirms the unexported base type provides default
// no-op OnStart / OnStop that return nil. It sits at the root of the
// embed chain so the chain-driver always finds an Actor at the bottom.
func TestBase_Lifecycle(t *testing.T) {
	b := base{}
	if err := b.OnStart(nil); err != nil {
		t.Fatalf("base.OnStart(nil): got %v, want nil", err)
	}
	if err := b.OnStop(nil); err != nil {
		t.Fatalf("base.OnStop(nil): got %v, want nil", err)
	}
}

// TestBase_IsTerminalNoOp confirms base.OnStart / OnStop have no
// observable side effects across repeated direct invocations.
func TestBase_IsTerminalNoOp(t *testing.T) {
	b := base{}
	for i := 0; i < 3; i++ {
		if err := b.OnStart(nil); err != nil {
			t.Fatalf("base.OnStart iteration %d: got %v, want nil", i, err)
		}
		if err := b.OnStop(nil); err != nil {
			t.Fatalf("base.OnStop iteration %d: got %v, want nil", i, err)
		}
	}
}

// TestBase_DoubleCallNoPanic confirms direct repeated invocation of
// base methods (without the chain driver) does NOT panic.
func TestBase_DoubleCallNoPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("base.OnStart double-call: unexpected panic %v", r)
		}
	}()
	b := base{}
	_ = b.OnStart(nil)
	_ = b.OnStart(nil)
	_ = b.OnStop(nil)
	_ = b.OnStop(nil)
}
