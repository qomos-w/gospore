package app

import (
	"context"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
)

// orderedSpawnRoot spawns one child ("parent") in OnInit and reports the
// start order, mirroring how sporecode's runtime wires SetChildStartOrder.
type orderedSpawnRoot struct {
	onInitComplete func(order []string, nameToID map[string]string)
}

func (r *orderedSpawnRoot) OnInit(ctx actor.Context) error {
	ref, err := ctx.Spawn(actor.PropsFromFunc(func() actor.Actor {
		return &orderedSpawnParent{}
	}), "parent")
	if err != nil {
		return err
	}
	if r.onInitComplete != nil {
		r.onInitComplete([]string{"parent"}, map[string]string{
			"parent": ref.ID().String(),
		})
	}
	return nil
}
func (r *orderedSpawnRoot) OnStart(actor.Context) error { return nil }
func (r *orderedSpawnRoot) OnStop(actor.Context) error  { return nil }
func (r *orderedSpawnRoot) Type() string                { return "orderedspawnroot" }

// orderedSpawnParent spawns a grandchild ("leaf") inside OnStart — the
// mid-batch dynamic spawn that previously lost its Start envelope forever.
type orderedSpawnParent struct{ leafStarted chan struct{} }

func (p *orderedSpawnParent) OnInit(ctx actor.Context) error {
	p.leafStarted = make(chan struct{})
	return nil
}
func (p *orderedSpawnParent) OnStart(ctx actor.Context) error {
	_, err := ctx.Spawn(actor.PropsFromFunc(func() actor.Actor {
		return &orderedSpawnLeaf{started: leafStartedCh}
	}), "leaf")
	return err
}
func (p *orderedSpawnParent) OnStop(actor.Context) error { return nil }
func (p *orderedSpawnParent) Type() string               { return "orderedspawnparent" }

var leafStartedCh = make(chan struct{})

type orderedSpawnLeaf struct{ started chan struct{} }

func (l *orderedSpawnLeaf) OnInit(actor.Context) error  { return nil }
func (l *orderedSpawnLeaf) OnStart(actor.Context) error { close(l.started); return nil }
func (l *orderedSpawnLeaf) OnStop(actor.Context) error  { return nil }
func (l *orderedSpawnLeaf) Type() string                { return "orderedspawleaf" }

// TestOrderedStartDeliversStartToMidBatchGrandchild: with ordered child
// startup, an actor that spawns a grandchild inside OnStart (during the
// batch, before batchInitComplete) must still see that grandchild started.
// Regression for the sporecode gen-manifest hang: allocate skipped Start
// delivery pre-batch and nothing re-delivered it, so WaitForAllCellsStart
// timed out forever.
func TestOrderedStartDeliversStartToMidBatchGrandchild(t *testing.T) {
	leafStartedCh = make(chan struct{})

	var appRef App
	root := &orderedSpawnRoot{
		onInitComplete: func(order []string, nameToID map[string]string) {
			if impl, ok := appRef.(*appImpl); ok {
				impl.SetChildStartOrder(order, nameToID)
			}
		},
	}

	a, err := New(
		WithNamespace("test"),
		WithRootActor(func() actor.Actor { return root }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	appRef = a

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	if err := a.WaitForAllCellsStart(5 * time.Second); err != nil {
		t.Fatalf("WaitForAllCellsStart: %v", err)
	}
	select {
	case <-leafStartedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("grandchild spawned during ordered batch never started")
	}
	cancel()
	<-done
}
