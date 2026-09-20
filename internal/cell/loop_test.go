package cell

import (
	"testing"

	"github.com/qomos-w/gospore/actor"
)

// TestRegisterLoop_StoresMode verifies that RegisterLoop records the handler
// mode alongside the loop name so runtime routing can enforce boundaries.
func TestRegisterLoop_StoresMode(t *testing.T) {
	c := newCellWithBus(t)
	ctx := NewStartContextForTest(c)

	if err := ctx.RegisterLoop("custom.exec", actor.ModeStateless); err != nil {
		t.Fatalf("RegisterLoop: %v", err)
	}

	c.loopsMu.Lock()
	lane, ok := c.loops["custom.exec"]
	c.loopsMu.Unlock()
	if !ok {
		t.Fatal("custom loop was not recorded")
	}
	if lane == nil {
		t.Fatal("loop lane is nil")
	}
	if lane.mode != actor.ModeStateless {
		t.Fatalf("loop mode = %v, want ModeStateless", lane.mode)
	}
	if lane.q != nil {
		t.Fatal("loop lane queue should not be initialized yet")
	}
}

// TestRegisterLoop_IdempotentPreservesFirstMode verifies that repeated
// RegisterLoop calls with different modes keep the first mode.
func TestRegisterLoop_IdempotentPreservesFirstMode(t *testing.T) {
	c := newCellWithBus(t)
	ctx := NewStartContextForTest(c)

	if err := ctx.RegisterLoop("custom.exec", actor.ModeStateful); err != nil {
		t.Fatalf("RegisterLoop first: %v", err)
	}
	if err := ctx.RegisterLoop("custom.exec", actor.ModeStateless); err != nil {
		t.Fatalf("RegisterLoop second: %v", err)
	}

	c.loopsMu.Lock()
	lane := c.loops["custom.exec"]
	c.loopsMu.Unlock()
	if lane.mode != actor.ModeStateful {
		t.Fatalf("loop mode = %v, want ModeStateful (first wins)", lane.mode)
	}
}

// TestRegister_StatefulHandlerToStatelessLoopFails verifies that registering
// a stateful handler targeting a stateless custom loop is rejected at
// registration time.
func TestRegister_StatefulHandlerToStatelessLoopFails(t *testing.T) {
	c := newCellWithBus(t)
	ctx := NewStartContextForTest(c)

	if err := ctx.RegisterLoop("custom.pure", actor.ModeStateless); err != nil {
		t.Fatalf("RegisterLoop: %v", err)
	}

	err := ctx.Register("test.stateful", func(actor.Context) error { return nil }, actor.WithLoop("custom.pure"))
	if err == nil {
		t.Fatal("expected register to fail for stateful handler on stateless loop")
	}
	if !contains(err.Error(), "mode mismatch") {
		t.Fatalf("error = %q, want containing 'mode mismatch'", err)
	}
}

// TestRegister_StatelessHandlerToStatefulLoopOK verifies that registering a
// stateless handler targeting a stateful custom loop succeeds.
func TestRegister_StatelessHandlerToStatefulLoopOK(t *testing.T) {
	c := newCellWithBus(t)
	ctx := NewStartContextForTest(c)

	if err := ctx.RegisterLoop("custom.exec", actor.ModeStateful); err != nil {
		t.Fatalf("RegisterLoop: %v", err)
	}

	err := ctx.Register("test.stateless", func(actor.PureContext) error { return nil }, actor.WithLoop("custom.exec"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestRegister_ScriptStatefulToStatelessLoopFails verifies that script
// registration is also guarded by loop/mode consistency.
func TestRegister_ScriptStatefulToStatelessLoopFails(t *testing.T) {
	c := newCellWithBus(t)
	ctx := NewStartContextForTest(c)

	if err := ctx.RegisterLoop("custom.pure", actor.ModeStateless); err != nil {
		t.Fatalf("RegisterLoop: %v", err)
	}

	err := ctx.RegisterScript("test.script", "return 1", actor.ModeStateful, actor.WithLoop("custom.pure"))
	if err == nil {
		t.Fatal("expected register script to fail for stateful script on stateless loop")
	}
}

func contains(s, substr string) bool { return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsAt(s, substr)) }
func containsAt(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
