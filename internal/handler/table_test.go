package handler

import (
	"strings"
	"testing"

	"github.com/qomos-w/gospore/actor"
)

// Named struct types for strict-mode fixtures. Strict mode rejects
// anonymous structs (no stable schema ID), so test cases that exercise
// the success path must declare named types.
type strictTestReq struct{ Name string }
type strictTestReqA struct{ A string }
type strictTestReqB struct{ B string }
type strictTestResp struct{ Result string }

func TestTable_Register_Visibility(t *testing.T) {
	tbl := NewTable()

	fn := func(ctx actor.Context, req struct{ Name string }) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}

	if err := tbl.Register("test.api.greet", fn, actor.Public()); err != nil {
		t.Fatalf("Register: %v", err)
	}

	vis, ok := tbl.Visibility("test.api.greet")
	if !ok {
		t.Fatal("Visibility lookup failed")
	}
	if vis != actor.VisibilityPublic {
		t.Errorf("visibility = %v, want VisibilityPublic", vis)
	}
}

func TestTable_Register_DefaultVisibility(t *testing.T) {
	tbl := NewTable()

	fn := func(ctx actor.Context, req struct{ Name string }) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}

	if err := tbl.Register("test.api.greet", fn); err != nil {
		t.Fatalf("Register: %v", err)
	}

	vis, ok := tbl.Visibility("test.api.greet")
	if !ok {
		t.Fatal("Visibility lookup failed")
	}
	if vis != actor.VisibilityInternal {
		t.Errorf("default visibility = %v, want VisibilityInternal", vis)
	}
}

func TestTable_Register_VisibilityMismatch(t *testing.T) {
	tbl := NewTable()

	fn := func(ctx actor.Context, req struct{ Name string }) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}

	if err := tbl.Register("test.api.greet", fn, actor.Public()); err != nil {
		t.Fatalf("first Register: %v", err)
	}

	// Re-register with different visibility should fail
	if err := tbl.Register("test.api.greet", fn, actor.AdminOnly()); err == nil {
		t.Fatal("expected error for visibility mismatch")
	}
}

func TestTable_Register_VisibilityConsistent(t *testing.T) {
	tbl := NewTable()

	fn := func(ctx actor.Context, req struct{ Name string }) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}

	if err := tbl.Register("test.api.greet", fn, actor.Public()); err != nil {
		t.Fatalf("first Register: %v", err)
	}

	// Re-register with same visibility should succeed (idempotent)
	if err := tbl.Register("test.api.greet", fn, actor.Public()); err != nil {
		t.Fatalf("second Register: %v", err)
	}
}

func TestTable_Register_LoopMetadata(t *testing.T) {
	tbl := NewTable()

	fn := func(ctx actor.Context, req struct{ Name string }) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}

	if err := tbl.Register("test.api.greet", fn, actor.WithLoop("custom.exec")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	inv, ok := tbl.Lookup("test.api.greet")
	if !ok || inv == nil {
		t.Fatal("Lookup failed")
	}
	if inv.Loop != "custom.exec" {
		t.Fatalf("loop = %q, want %q", inv.Loop, "custom.exec")
	}
}

func TestTable_Register_DefaultLoopMetadata(t *testing.T) {
	tbl := NewTable()

	stateful := func(ctx actor.Context, req struct{ Name string }) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}
	stateless := func(ctx actor.PureContext, req struct{ Name string }) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}

	if err := tbl.Register("test.api.stateful", stateful); err != nil {
		t.Fatalf("Register stateful: %v", err)
	}
	if err := tbl.Register("test.api.stateless", stateless); err != nil {
		t.Fatalf("Register stateless: %v", err)
	}

	statefulInv, ok := tbl.Lookup("test.api.stateful")
	if !ok || statefulInv == nil {
		t.Fatal("Lookup stateful failed")
	}
	if statefulInv.Loop != actor.DefaultLoopOwner {
		t.Fatalf("stateful loop = %q, want %q", statefulInv.Loop, actor.DefaultLoopOwner)
	}

	statelessInv, ok := tbl.Lookup("test.api.stateless")
	if !ok || statelessInv == nil {
		t.Fatal("Lookup stateless failed")
	}
	if statelessInv.Loop != actor.DefaultLoopPure {
		t.Fatalf("stateless loop = %q, want %q", statelessInv.Loop, actor.DefaultLoopPure)
	}
}

func TestTable_Register_ExplicitModeMustMatchSignature(t *testing.T) {
	tbl := NewTable()

	fn := func(ctx actor.Context, req struct{ Name string }) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}

	if err := tbl.Register("test.api.greet", fn, actor.WithMode(actor.ModeStateless)); err == nil {
		t.Fatal("expected mode mismatch")
	}
}

func TestTable_RegisterScript_Visibility(t *testing.T) {
	tbl := NewTable()

	if err := tbl.RegisterScript("test.api.script", "source", actor.ModeStateful, actor.AdminOnly()); err != nil {
		t.Fatalf("RegisterScript: %v", err)
	}

	vis, ok := tbl.Visibility("test.api.script")
	if !ok {
		t.Fatal("Visibility lookup failed")
	}
	if vis != actor.VisibilityAdmin {
		t.Errorf("visibility = %v, want VisibilityAdmin", vis)
	}
}

func TestTable_RegisterScript_VisibilityMismatch(t *testing.T) {
	tbl := NewTable()

	if err := tbl.RegisterScript("test.api.script", "source1", actor.ModeStateful, actor.Public()); err != nil {
		t.Fatalf("first RegisterScript: %v", err)
	}

	if err := tbl.RegisterScript("test.api.script", "source2", actor.ModeStateful, actor.AdminOnly()); err == nil {
		t.Fatal("expected error for visibility mismatch")
	}
}

func TestTable_RegisterScript_LoopMismatch(t *testing.T) {
	tbl := NewTable()

	if err := tbl.RegisterScript("test.api.script", "source1", actor.ModeStateful, actor.WithLoop("owner")); err != nil {
		t.Fatalf("first RegisterScript: %v", err)
	}

	err := tbl.RegisterScript("test.api.script", "source2", actor.ModeStateful, actor.WithLoop("other"))
	if err == nil {
		t.Fatal("expected error for loop mismatch")
	}
	if !strings.Contains(err.Error(), actor.DiagCallableLoopMismatch) {
		t.Fatalf("error = %q, want containing %q", err.Error(), actor.DiagCallableLoopMismatch)
	}
}

func TestTable_Visibility_Miss(t *testing.T) {
	tbl := NewTable()
	vis, ok := tbl.Visibility("missing")
	if ok {
		t.Error("Visibility on empty table should return false")
	}
	if vis != actor.VisibilityInternal {
		t.Errorf("zero visibility = %v, want VisibilityInternal", vis)
	}
}

func TestTable_Visibility_Nil(t *testing.T) {
	var tbl *Table
	vis, ok := tbl.Visibility("anything")
	if ok {
		t.Error("Visibility on nil table should return false")
	}
	if vis != actor.VisibilityInternal {
		t.Errorf("nil visibility = %v, want VisibilityInternal", vis)
	}
}

func TestStrictTable_Register_StructParamRequired(t *testing.T) {
	tbl := NewStrictTable()

	scalarParam := func(ctx actor.Context, name string) (strictTestResp, error) {
		return strictTestResp{}, nil
	}
	if err := tbl.Register("test.scalar_param", scalarParam); err == nil {
		t.Fatal("expected error for scalar request parameter")
	}

	multiParam := func(ctx actor.Context, a strictTestReqA, b strictTestReqB) (strictTestResp, error) {
		return strictTestResp{}, nil
	}
	if err := tbl.Register("test.multi_param", multiParam); err == nil {
		t.Fatal("expected error for multiple request parameters")
	}

	structParam := func(ctx actor.Context, req strictTestReq) (strictTestResp, error) {
		return strictTestResp{}, nil
	}
	if err := tbl.Register("test.struct_param", structParam); err != nil {
		t.Fatalf("unexpected error for struct parameter: %v", err)
	}
}

func TestStrictTable_Register_NoParamAllowed(t *testing.T) {
	tbl := NewStrictTable()

	noParam := func(ctx actor.Context) error { return nil }
	if err := tbl.Register("test.no_param", noParam); err != nil {
		t.Fatalf("unexpected error for zero-parameter callable: %v", err)
	}
}

func TestStrictTable_Register_StreamingStructParamRequired(t *testing.T) {
	tbl := NewStrictTable()

	badStream := func(ctx actor.Context, name string, emit actor.Emitter) error { return nil }
	if err := tbl.Register("test.bad_stream", badStream, actor.Streaming[struct{}]()); err == nil {
		t.Fatal("expected error for scalar streaming request parameter")
	}

	goodStream := func(ctx actor.Context, req strictTestReq, emit actor.Emitter) error { return nil }
	if err := tbl.Register("test.good_stream", goodStream, actor.Streaming[struct{}]()); err != nil {
		t.Fatalf("unexpected error for struct streaming parameter: %v", err)
	}
}

func TestStrictTable_Register_NonStrictTableAllowsScalarParam(t *testing.T) {
	tbl := NewTable()

	scalarParam := func(ctx actor.Context, name string) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}
	if err := tbl.Register("test.scalar_param", scalarParam); err != nil {
		t.Fatalf("non-strict table should allow scalar parameter: %v", err)
	}
}

// TestStrictTable_Register_RejectsAnonymousStruct verifies that strict
// mode rejects anonymous struct types in both the request parameter and
// the return value. Anonymous structs produce orphan schema IDs in the
// manifest because their ClassName is empty — manifest export drops the
// schema entry while the callable still references the ID, breaking
// binary codec lookup at the wire layer.
func TestStrictTable_Register_RejectsAnonymousStruct(t *testing.T) {
	tbl := NewStrictTable()

	anonymousReq := func(ctx actor.Context, req struct{ Name string }) (strictTestResp, error) {
		return strictTestResp{}, nil
	}
	err := tbl.Register("test.anon_req", anonymousReq)
	if err == nil {
		t.Fatal("expected error for anonymous struct request parameter")
	}
	if !strings.Contains(err.Error(), actor.DiagCallableAnonymousStruct) {
		t.Fatalf("anonymous param error = %q, want containing %q", err.Error(), actor.DiagCallableAnonymousStruct)
	}

	anonymousRet := func(ctx actor.Context, req strictTestReq) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}
	err = tbl.Register("test.anon_ret", anonymousRet)
	if err == nil {
		t.Fatal("expected error for anonymous struct return")
	}
	if !strings.Contains(err.Error(), actor.DiagCallableAnonymousStruct) {
		t.Fatalf("anonymous return error = %q, want containing %q", err.Error(), actor.DiagCallableAnonymousStruct)
	}

	anonymousStreamReq := func(ctx actor.Context, req struct{ Name string }, emit actor.Emitter) error { return nil }
	err = tbl.Register("test.anon_stream_req", anonymousStreamReq, actor.Streaming[struct{}]())
	if err == nil {
		t.Fatal("expected error for anonymous struct streaming request parameter")
	}
	if !strings.Contains(err.Error(), actor.DiagCallableAnonymousStruct) {
		t.Fatalf("anonymous stream param error = %q, want containing %q", err.Error(), actor.DiagCallableAnonymousStruct)
	}
}

func TestTable_StampServiceForDomain(t *testing.T) {
	tbl := NewTable()

	fn := func(ctx actor.Context, req struct{ Name string }) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}
	if err := tbl.Register("appmanager.list", fn); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := tbl.Register("appmanager.apps.get", fn); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := tbl.Register("filesystem.read", fn); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := tbl.RegisterWithService("tools.run", fn, "explicit"); err != nil {
		t.Fatalf("Register explicit: %v", err)
	}
	if err := tbl.Register("list_callables", fn); err != nil {
		t.Fatalf("Register flat: %v", err)
	}

	if n := tbl.StampServiceForDomain("appmanager"); n != 2 {
		t.Fatalf("StampServiceForDomain stamped %d, want 2", n)
	}
	want := map[string]string{
		"appmanager.list":     "appmanager",
		"appmanager.apps.get": "appmanager",
		"filesystem.read":     "",
		"tools.run":           "explicit",
		"list_callables":      "",
	}
	for callID, wantSvc := range want {
		inv, ok := tbl.Lookup(callID)
		if !ok {
			t.Fatalf("%q not registered", callID)
		}
		if inv.ServiceName != wantSvc {
			t.Errorf("%q ServiceName = %q, want %q", callID, inv.ServiceName, wantSvc)
		}
	}

	// Idempotent: stamping again changes nothing and reports zero.
	if n := tbl.StampServiceForDomain("appmanager"); n != 0 {
		t.Fatalf("second StampServiceForDomain stamped %d, want 0", n)
	}
	inv, _ := tbl.Lookup("appmanager.list")
	if inv.ServiceName != "appmanager" {
		t.Fatalf("appmanager.list ServiceName = %q, want appmanager", inv.ServiceName)
	}
}

func TestTable_StampServiceForDomain_ExactSegmentOnly(t *testing.T) {
	tbl := NewTable()

	fn := func(ctx actor.Context, req struct{ Name string }) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}
	if err := tbl.Register("appmanager.list", fn); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Prefix-nested domain names must not collide.
	if n := tbl.StampServiceForDomain("app"); n != 0 {
		t.Fatalf("StampServiceForDomain(\"app\") stamped %d, want 0", n)
	}
	inv, _ := tbl.Lookup("appmanager.list")
	if inv.ServiceName != "" {
		t.Fatalf("appmanager.list ServiceName = %q, want empty", inv.ServiceName)
	}
}

func TestTable_RegisterScript_StoresServiceName(t *testing.T) {
	tbl := NewTable()

	if err := tbl.RegisterScriptWithService("appmanager.spawn", "# src", actor.ModeStateless, "appmanager"); err != nil {
		t.Fatalf("RegisterScript: %v", err)
	}
	inv, ok := tbl.Lookup("appmanager.spawn")
	if !ok {
		t.Fatal("script invoker missing")
	}
	if inv.ServiceName != "appmanager" {
		t.Fatalf("ServiceName = %q, want appmanager", inv.ServiceName)
	}

	// Default stays empty when no service is injected.
	if err := tbl.RegisterScript("plan.submit", "# src", actor.ModeStateless); err != nil {
		t.Fatalf("RegisterScript: %v", err)
	}
	inv2, _ := tbl.Lookup("plan.submit")
	if inv2.ServiceName != "" {
		t.Fatalf("ServiceName = %q, want empty", inv2.ServiceName)
	}
}

func TestTable_RegisterScript_ReRegisterBackfillsEmptyService(t *testing.T) {
	tbl := NewTable()

	if err := tbl.RegisterScript("appmanager.spawn", "# v1", actor.ModeStateless); err != nil {
		t.Fatalf("RegisterScript: %v", err)
	}
	// Re-register now carries a (derived) service; empty must be filled in.
	if err := tbl.RegisterScriptWithService("appmanager.spawn", "# v2", actor.ModeStateless, "appmanager"); err != nil {
		t.Fatalf("RegisterScript re-register: %v", err)
	}
	inv, _ := tbl.Lookup("appmanager.spawn")
	if inv.ServiceName != "appmanager" {
		t.Fatalf("ServiceName = %q, want appmanager", inv.ServiceName)
	}

	// A later explicit re-declare must not overwrite the existing value.
	if err := tbl.RegisterScriptWithService("appmanager.spawn", "# v3", actor.ModeStateless, "other"); err != nil {
		t.Fatalf("RegisterScript re-declare: %v", err)
	}
	inv, _ = tbl.Lookup("appmanager.spawn")
	if inv.ServiceName != "appmanager" {
		t.Fatalf("ServiceName = %q, want appmanager (never overwritten)", inv.ServiceName)
	}
}
