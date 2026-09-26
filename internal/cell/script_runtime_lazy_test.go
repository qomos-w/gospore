package cell

import (
	"reflect"
	"testing"

	"github.com/qomos-w/gospore/projection"
	"github.com/qomos-w/spore/script"
)

type DeferComp struct {
	Value int `gospore:"component"`
}

// bindActorComponents must only record the slots when no Runtime exists:
// creating one eagerly would allocate a VM heap for every component-bearing
// cell even if it never runs spore code.
func TestBindActorComponentsDefersRuntimeCreation(t *testing.T) {
	o := &scriptRuntimeOwner{namespace: "test"}
	slots := []projection.ComponentSlot{{
		Name: "DeferComp",
		Type: reflect.TypeOf(DeferComp{}),
	}}

	if err := o.bindActorComponents(slots); err != nil {
		t.Fatalf("bindActorComponents: %v", err)
	}
	if o.spore != nil {
		t.Fatal("runtime created before first spore use")
	}

	// First real spore use creates the runtime and applies the pending
	// component bindings recorded earlier.
	if err := o.ensureRuntime(); err != nil {
		t.Fatalf("ensureRuntime: %v", err)
	}
	if o.spore == nil {
		t.Fatal("runtime missing after ensureRuntime")
	}
}

// When a runtime already exists (spore module loaded, or the test-only
// setSporeRuntime hook), bindActorComponents binds immediately as before.
func TestBindActorComponentsBindsImmediatelyOnExistingRuntime(t *testing.T) {
	o := &scriptRuntimeOwner{namespace: "test"}
	rt, err := script.NewRuntime()
	if err != nil {
		t.Fatalf("script.NewRuntime: %v", err)
	}
	defer rt.Close()
	o.setSporeRuntime(rt)

	slots := []projection.ComponentSlot{{
		Name: "DeferComp",
		Type: reflect.TypeOf(DeferComp{}),
	}}
	if err := o.bindActorComponents(slots); err != nil {
		t.Fatalf("bindActorComponents: %v", err)
	}
	if len(o.componentSlots) != 1 || o.componentSlots[0].Name != "DeferComp" {
		t.Fatalf("component slots not recorded: %+v", o.componentSlots)
	}
}
