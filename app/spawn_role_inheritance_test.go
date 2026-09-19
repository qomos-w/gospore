package app

import (
	"context"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
)

type roleSpawnRoot struct {
	ownerSpawned chan id.ActorID
}

func (r *roleSpawnRoot) OnInit(ctx actor.Context) error {
	ref, err := ctx.Spawn(actor.PropsFromFunc(func() actor.Actor {
		return &roleOwnerActor{}
	}).WithRole("agent").WithPlanner(), "role-owner")
	if err != nil {
		return err
	}
	r.ownerSpawned <- ref.ID()
	return nil
}
func (r *roleSpawnRoot) OnStart(actor.Context) error { return nil }
func (r *roleSpawnRoot) OnStop(actor.Context) error  { return nil }
func (r *roleSpawnRoot) Type() string                { return "rolespawnroot" }

type roleOwnerActor struct{}

func (o *roleOwnerActor) OnInit(ctx actor.Context) error { return nil }
func (o *roleOwnerActor) OnStart(ctx actor.Context) error {
	ref, err := ctx.Spawn(actor.PropsFromFunc(func() actor.Actor {
		return &roleDerivedActor{}
	}), "derived")
	if err != nil {
		return err
	}
	roleDerivedChan <- ref.ID()
	return nil
}
func (o *roleOwnerActor) OnStop(actor.Context) error  { return nil }
func (o *roleOwnerActor) Type() string                { return "roleowneractor" }

type roleDerivedActor struct{}

func (d *roleDerivedActor) OnInit(actor.Context) error  { return nil }
func (d *roleDerivedActor) OnStart(actor.Context) error { return nil }
func (d *roleDerivedActor) OnStop(actor.Context) error  { return nil }
func (d *roleDerivedActor) Type() string                { return "rolederivedactor" }

var roleDerivedChan = make(chan id.ActorID, 1)

// TestSpawnInheritsParentRole verifies that children spawned without an
// explicit role (plan nodes, lanes, delegation children) inherit the parent
// cell's role, so their outbound planner calls carry the parent's caller
// identity instead of a zero identity.
func TestSpawnInheritsParentRole(t *testing.T) {
	roleDerivedChan = make(chan id.ActorID, 1)
	root := &roleSpawnRoot{ownerSpawned: make(chan id.ActorID, 1)}
	a, err := New(
		WithNamespace("test"),
		WithRootActor(func() actor.Actor { return root }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	if err := a.WaitForAllCellsStart(5 * time.Second); err != nil {
		t.Fatalf("WaitForAllCellsStart: %v", err)
	}

	impl := a.(*appImpl)
	ownerID := <-root.ownerSpawned
	if cell := impl.getCell(ownerID); cell == nil || cell.Role() != "agent" {
		t.Fatalf("role-owner cell = %v, want role \"agent\"", cell)
	}

	select {
	case derivedID := <-roleDerivedChan:
		cell := impl.getCell(derivedID)
		if cell == nil {
			t.Fatalf("derived cell %v not found", derivedID)
		}
		if cell.Role() != "agent" {
			t.Fatalf("derived cell role = %q, want inherited \"agent\"", cell.Role())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for derived child")
	}
	cancel()
	<-done
}
