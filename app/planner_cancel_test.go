package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/invoke"
)

// plannerCancelChild pins the cancellation-cause contract of
// planner.Stream / planner.Call: when the CONSUMING context is
// cancelled, the promise must reject with that context's error
// (context.Canceled / DeadlineExceeded), not the bare
// invoke.ErrCallCancelled sentinel — downstream consumers classify
// pause/stop/timeout via errors.Is(err, context.Canceled).
type plannerCancelChild struct {
	actor.Host
	streamErr chan error
	callErr   chan error
}

func (a *plannerCancelChild) OnStart(ctx actor.Context) error {
	if err := a.Host.OnStart(ctx); err != nil {
		return err
	}

	// Slow callees are stateless so they run on fork goroutines and
	// cannot serialize behind the driver handler on the owner lane.
	if err := ctx.Register("slow.stream", func(ctx actor.PureContext, _ string, em actor.Emitter) error {
		for i := 0; ; i++ {
			if err := em.Send(i); err != nil {
				return err
			}
			select {
			case <-ctx.Lifecycle().Done():
				return nil
			case <-time.After(20 * time.Millisecond):
			}
		}
	}); err != nil {
		return err
	}
	if err := actor.RegisterStateless[string, string](ctx, "slow.unary",
		func(_ actor.PureContext, _ string) (string, error) {
			time.Sleep(500 * time.Millisecond)
			return "late", nil
		}); err != nil {
		return err
	}

	if err := actor.RegisterStateful[string, string](ctx, "drive.stream",
		func(ctx actor.Context, _ string) (string, error) {
			cctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			time.AfterFunc(50*time.Millisecond, cancel)
			p := ctx.Planner().Stream(cctx, ctx.Self(), "slow.stream", "", func(any) error { return nil })
			_, err := p.Await()
			a.streamErr <- err
			return "driven", nil
		}); err != nil {
		return err
	}
	if err := actor.RegisterStateful[string, string](ctx, "drive.call",
		func(ctx actor.Context, _ string) (string, error) {
			cctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			time.AfterFunc(50*time.Millisecond, cancel)
			p := ctx.Planner().Call(cctx, ctx.Self(), "slow.unary", "")
			_, err := p.Await()
			a.callErr <- err
			return "driven", nil
		}); err != nil {
		return err
	}
	return ctx.RegisterDomain("pcdriver").Expose()
}

type plannerCancelRoot struct {
	actor.Host
	child *plannerCancelChild
}

func (r *plannerCancelRoot) OnInit(ctx actor.Context) error {
	_, err := ctx.Spawn(actor.PropsFromFunc(func() actor.Actor { return r.child }).WithPlanner(), "driver")
	return err
}

func TestPlannerCancel_RejectsWithContextCause(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	child := &plannerCancelChild{
		streamErr: make(chan error, 1),
		callErr:   make(chan error, 1),
	}
	a, err := New(
		WithNamespace("appcancel"),
		WithRootActor(func() actor.Actor { return &plannerCancelRoot{child: child} }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	defer func() { cancel(); <-done }()

	time.Sleep(150 * time.Millisecond)

	driver, ok := a.LookupService("pcdriver")
	if !ok {
		t.Fatal("pcdriver service not resolvable after boot")
	}

	cases := []struct {
		name  string
		drive string
		errCh chan error
	}{
		{"stream", "drive.stream", child.streamErr},
		{"call", "drive.call", child.callErr},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			call := driver.Invoke(ctx, tc.drive, "")
			val, err := call.Recv()
			if err != nil || val == nil {
				t.Fatalf("driver invoke: val=%v err=%v", val, err)
			}
			select {
			case perr := <-tc.errCh:
				if !errors.Is(perr, context.Canceled) {
					t.Fatalf("planner rejection: got %v, want context.Canceled", perr)
				}
				if errors.Is(perr, invoke.ErrCallCancelled) {
					t.Fatalf("planner rejection leaked the bare sentinel: %v", perr)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("planner promise never settled after ctx cancellation")
			}
		})
	}
}
