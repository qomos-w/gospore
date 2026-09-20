package cell

import (
	"testing"

	"github.com/qomos-w/gospore/actor"
)

// lookupService returns the stored ServiceName of a registered invoker.
func lookupService(t *testing.T, c *Cell, callID string) string {
	t.Helper()
	tbl := c.handlers
	if tbl == nil {
		t.Fatalf("no handler table for %q", callID)
	}
	inv, ok := tbl.Lookup(callID)
	if !ok || inv == nil {
		t.Fatalf("invoker %q not registered", callID)
	}
	return inv.ServiceName
}

// TestRegister_BeforeRegisterDomain_BackfillsService verifies the
// "Register before RegisterDomain" ordering: the invoker starts with an
// empty ServiceName and the later domain declaration stamps it.
func TestRegister_BeforeRegisterDomain_BackfillsService(t *testing.T) {
	c := newCellWithBus(t)
	ctx := NewStartContextForTest(c)

	fn := func(ctx actor.Context, req struct{ Name string }) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}
	if err := ctx.Register("appmanager.list", fn); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if got := lookupService(t, c, "appmanager.list"); got != "" {
		t.Fatalf("ServiceName before RegisterDomain = %q, want empty", got)
	}

	ctx.RegisterDomain("appmanager")
	if got := lookupService(t, c, "appmanager.list"); got != "appmanager" {
		t.Fatalf("ServiceName after RegisterDomain = %q, want appmanager", got)
	}
}

// TestRegister_AfterRegisterDomain_DerivesService verifies the forward
// derivation: a domain declared first stamps registrations that follow.
func TestRegister_AfterRegisterDomain_DerivesService(t *testing.T) {
	c := newCellWithBus(t)
	ctx := NewStartContextForTest(c)

	ctx.RegisterDomain("appmanager")
	fn := func(ctx actor.Context, req struct{ Name string }) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}
	if err := ctx.Register("appmanager.list", fn); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if got := lookupService(t, c, "appmanager.list"); got != "appmanager" {
		t.Fatalf("ServiceName = %q, want appmanager", got)
	}
}

// TestRegisterScript_AfterRegisterDomain_DerivesService verifies script
// registrations also pick up the derived service name.
func TestRegisterScript_AfterRegisterDomain_DerivesService(t *testing.T) {
	c := newCellWithBus(t)
	ctx := NewStartContextForTest(c)

	ctx.RegisterDomain("appmanager")
	if err := ctx.RegisterScript("appmanager.spawn", "# script", actor.ModeStateless); err != nil {
		t.Fatalf("RegisterScript: %v", err)
	}
	if got := lookupService(t, c, "appmanager.spawn"); got != "appmanager" {
		t.Fatalf("ServiceName = %q, want appmanager", got)
	}
}

// TestRegisterScript_BeforeRegisterDomain_BackfillsService verifies the
// backfill path also covers script invokers registered before the domain.
func TestRegisterScript_BeforeRegisterDomain_BackfillsService(t *testing.T) {
	c := newCellWithBus(t)
	ctx := NewStartContextForTest(c)

	if err := ctx.RegisterScript("appmanager.spawn", "# script", actor.ModeStateless); err != nil {
		t.Fatalf("RegisterScript: %v", err)
	}
	if got := lookupService(t, c, "appmanager.spawn"); got != "" {
		t.Fatalf("ServiceName before RegisterDomain = %q, want empty", got)
	}
	ctx.RegisterDomain("appmanager")
	if got := lookupService(t, c, "appmanager.spawn"); got != "appmanager" {
		t.Fatalf("ServiceName after RegisterDomain = %q, want appmanager", got)
	}
}

// TestRegister_DerivedServiceWithOtherOpts verifies the derived service
// name is injected even when other register options are present: with
// WithService gone, RegisterDomain derivation is the only source of the
// route service name.
func TestRegister_DerivedServiceWithOtherOpts(t *testing.T) {
	c := newCellWithBus(t)
	ctx := NewStartContextForTest(c)

	fn := func(ctx actor.Context, req struct{ Name string }) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}
	ctx.RegisterDomain("appmanager")
	if err := ctx.Register("appmanager.list", fn, actor.Public(), actor.WithDescription("list apps")); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if got := lookupService(t, c, "appmanager.list"); got != "appmanager" {
		t.Fatalf("ServiceName = %q, want appmanager (derived alongside other options)", got)
	}
	// Re-declaring the domain must not change the derived value.
	ctx.RegisterDomain("appmanager")
	if got := lookupService(t, c, "appmanager.list"); got != "appmanager" {
		t.Fatalf("ServiceName after re-declare = %q, want appmanager", got)
	}
}

// TestRegisterDomain_BackfillIdempotent verifies repeated domain
// declarations keep a single stable value.
func TestRegisterDomain_BackfillIdempotent(t *testing.T) {
	c := newCellWithBus(t)
	ctx := NewStartContextForTest(c)

	fn := func(ctx actor.Context, req struct{ Name string }) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}
	if err := ctx.Register("appmanager.list", fn); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ctx.RegisterDomain("appmanager")
	ctx.RegisterDomain("appmanager")
	if got := lookupService(t, c, "appmanager.list"); got != "appmanager" {
		t.Fatalf("ServiceName = %q, want appmanager", got)
	}
}

// TestRegisterDomain_BackfillOnlyMatchesExactSegment verifies that flat
// callables (no dot) and non-matching segments are never stamped.
func TestRegisterDomain_BackfillOnlyMatchesExactSegment(t *testing.T) {
	c := newCellWithBus(t)
	ctx := NewStartContextForTest(c)

	fn := func(ctx actor.Context, req struct{ Name string }) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}
	if err := ctx.Register("list_callables", fn); err != nil {
		t.Fatalf("Register flat: %v", err)
	}
	if err := ctx.Register("appmanager.list", fn); err != nil {
		t.Fatalf("Register domain-shaped: %v", err)
	}
	ctx.RegisterDomain("agent")
	if got := lookupService(t, c, "list_callables"); got != "" {
		t.Fatalf("flat callID ServiceName = %q, want empty", got)
	}
	if got := lookupService(t, c, "appmanager.list"); got != "" {
		t.Fatalf("unrelated domain ServiceName = %q, want empty", got)
	}
}
