package app

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
)

type typedReq struct {
	V int `json:"v"`
}

type typedResp struct {
	Dbl int `json:"dbl"`
}

// typedModeActor registers one handler per execution mode through the
// compile-time typed API.
type typedModeActor struct {
	actor.Host
	inFlight    atomic.Int64
	maxStateful atomic.Int64
	maxStateless atomic.Int64
}

func (a *typedModeActor) OnStart(ctx actor.Context) error {
	if err := a.Host.OnStart(ctx); err != nil {
		return err
	}
	if err := actor.RegisterStateful[typedReq, typedResp](ctx, "typed.stateful",
		func(_ actor.Context, req typedReq) (typedResp, error) {
			cur := a.inFlight.Add(1)
			for {
				max := a.maxStateful.Load()
				if cur <= max || a.maxStateful.CompareAndSwap(max, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			a.inFlight.Add(-1)
			return typedResp{Dbl: req.V * 2}, nil
		}); err != nil {
		return err
	}
	return actor.RegisterStateless[typedReq, typedResp](ctx, "typed.stateless",
		func(_ actor.PureContext, req typedReq) (typedResp, error) {
			cur := a.inFlight.Add(1)
			for {
				max := a.maxStateless.Load()
				if cur <= max || a.maxStateless.CompareAndSwap(max, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			a.inFlight.Add(-1)
			return typedResp{Dbl: req.V * 3}, nil
		})
}

// TestRegisterTyped_DispatchAndModes pins the contract of the typed
// registration API: results decode correctly end-to-end, stateful
// handlers serialize on the owner lane (in-flight never exceeds 1), and
// stateless handlers run on fork goroutines (concurrent in-flight > 1).
func TestRegisterTyped_DispatchAndModes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	a, err := New(
		WithNamespace("apptyped"),
		WithRootActor(func() actor.Actor { return &typedModeActor{} }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	defer func() { cancel(); <-done }()

	// Wait for boot (all callables registered).
	time.Sleep(100 * time.Millisecond)

	self := a.Self()

	// Unary results, both modes.
	call := self.Invoke(ctx, "typed.stateless", typedReq{V: 7})
	val, err := call.Recv()
	if err != nil {
		t.Fatalf("stateless invoke: %v", err)
	}
	if resp, ok := val.(typedResp); !ok || resp.Dbl != 21 {
		t.Fatalf("stateless result: got %#v, want typedResp{Dbl:21}", val)
	}

	// Concurrent stateful calls: the owner lane serializes them, so the
	// in-flight counter must never exceed 1.
	const n = 8
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			c := self.Invoke(ctx, "typed.stateful", typedReq{V: i})
			if _, err := c.Recv(); err != nil {
				errCh <- err
			} else {
				errCh <- nil
			}
		}()
	}
	for i := 0; i < n; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("stateful invoke: %v", err)
		}
	}
	if got := typedMaxStatefulOf(a); got > 1 {
		t.Fatalf("stateful lane saw %d concurrent handlers; owner lane must serialize", got)
	}

	// Concurrent stateless calls: fork goroutines overlap, so in-flight
	// must exceed 1 (serial execution would take 8*20ms; overlap proves
	// the pure lane).
	errCh2 := make(chan error, n)
	start := time.Now()
	for i := 0; i < n; i++ {
		go func() {
			c := self.Invoke(ctx, "typed.stateless", typedReq{V: i})
			if _, err := c.Recv(); err != nil {
				errCh2 <- err
			} else {
				errCh2 <- nil
			}
		}()
	}
	for i := 0; i < n; i++ {
		if err := <-errCh2; err != nil {
			t.Fatalf("stateless invoke: %v", err)
		}
	}
	if got := typedMaxStatelessOf(a); got < 2 {
		t.Fatalf("stateless lane never overlapped (max in-flight %d); handlers must run on fork goroutines", got)
	}
	if elapsed := time.Since(start); elapsed > time.Duration(n)*20*time.Millisecond {
		t.Fatalf("stateless batch took %v; serialized execution would be ~%v", elapsed, time.Duration(n)*20*time.Millisecond)
	}
}

func typedMaxStatefulOf(a App) int64 {
	ta, ok := a.(*appImpl)
	if !ok {
		return 0
	}
	root := ta.getCell(ta.Self().ID())
	if root == nil {
		return 0
	}
	if actor_, ok := root.Actor().(*typedModeActor); ok {
		return actor_.maxStateful.Load()
	}
	return 0
}

func typedMaxStatelessOf(a App) int64 {
	ta, ok := a.(*appImpl)
	if !ok {
		return 0
	}
	root := ta.getCell(ta.Self().ID())
	if root == nil {
		return 0
	}
	if actor_, ok := root.Actor().(*typedModeActor); ok {
		return actor_.maxStateless.Load()
	}
	return 0
}
