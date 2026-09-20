package promise

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestThenMethod tests the type-safe generic Then method.
func TestThenMethod(t *testing.T) {
	p := Resolve[int](42)
	result := p.Then(func(x int) string {
		return "value: " + string(rune(x))
	})

	val, err := result.Await()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if val != "value: *" {
		t.Errorf("expected 'value: *', got %q", val)
	}
}

// TestThenMethodRejects propagates errors through the Then method.
func TestThenMethodRejects(t *testing.T) {
	p := Reject[int](errors.New("boom"))
	result := p.Then(func(x int) string { return string(rune(x)) })

	if _, err := result.Await(); err == nil {
		t.Fatal("expected error to propagate through Then")
	}
}

// TestThenTyped tests the deprecated ThenTyped alias still works.
func TestThenTyped(t *testing.T) {
	p := Resolve[int](42)
	result := p.ThenTyped(func(x int) int {
		return x * 2
	})

	val, err := result.Await()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if val != 84 {
		t.Errorf("expected 84, got %d", val)
	}
}

// TestMapMethod tests the type-safe generic Map method.
func TestMapMethod(t *testing.T) {
	p := Resolve[int](10)
	result := p.Map(func(x int) float64 {
		return float64(x) * 1.5
	})

	val, err := result.Await()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if val != 15.0 {
		t.Errorf("expected 15.0, got %f", val)
	}
}

// TestFlatMapMethod tests the type-safe generic FlatMap method.
func TestFlatMapMethod(t *testing.T) {
	p := Resolve[int](5)
	result := p.FlatMap(func(x int) *Promise[string] {
		return Async[string](func(resolve func(string), reject func(any)) {
			time.Sleep(10 * time.Millisecond)
			resolve("result: " + string(rune(x+'0')))
		})
	})

	val, err := result.Await()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if val != "result: 5" {
		t.Errorf("expected 'result: 5', got %q", val)
	}
}

// TestFlatMapMethodRejects propagates errors from the inner promise.
func TestFlatMapMethodRejects(t *testing.T) {
	p := Resolve[int](5)
	result := p.FlatMap(func(x int) *Promise[string] {
		return Reject[string](errors.New("inner"))
	})

	if _, err := result.Await(); err == nil {
		t.Fatal("expected inner error to propagate")
	}
}

// TestCatchMethod tests type-safe Catch method recovery.
func TestCatchMethod(t *testing.T) {
	p := Reject[int](errors.New("test error"))

	result := p.Catch(func(err error) int {
		return -1
	})

	val, err := result.Await()
	if err != nil {
		t.Fatalf("expected no error after catch, got %v", err)
	}
	if val != -1 {
		t.Errorf("expected -1, got %d", val)
	}
}

// TestCatchMethodNoReject passes resolved value through unchanged.
func TestCatchMethodNoReject(t *testing.T) {
	p := Resolve[int](42)
	result := p.Catch(func(err error) int { return -1 })

	val, err := result.Await()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if val != 42 {
		t.Errorf("expected 42, got %d", val)
	}
}

// TestCatchTyped tests the deprecated CatchTyped alias still works.
func TestCatchTyped(t *testing.T) {
	p := Reject[int](errors.New("test error"))

	result := p.CatchTyped(func(err error) int {
		return -1
	})

	val, err := result.Await()
	if err != nil {
		t.Fatalf("expected no error after catch, got %v", err)
	}
	if val != -1 {
		t.Errorf("expected -1, got %d", val)
	}
}

// TestChainedMethods tests chaining multiple generic methods.
func TestChainedMethods(t *testing.T) {
	p := Resolve[int](10)

	// Chain: int -> int*2 -> float64 -> string
	result := p.
		Then(func(x int) int { return x * 2 }).
		Map(func(x int) float64 { return float64(x) + 0.5 }).
		Then(func(x float64) string { return "result" })

	val, err := result.Await()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if val != "result" {
		t.Errorf("expected 'result', got %q", val)
	}
}

// TestPackageTwinsDeprecated verifies the deprecated package-level twins still work.
func TestPackageTwinsDeprecated(t *testing.T) {
	// Then package function
	r1 := Then(Resolve[int](42), func(x int) string { return "v" })
	if v, err := r1.Await(); err != nil || v != "v" {
		t.Fatalf("package Then failed: %v %v", v, err)
	}
	// Map package function
	r2 := Map(Resolve[int](10), func(x int) float64 { return float64(x) * 1.5 })
	if v, err := r2.Await(); err != nil || v != 15.0 {
		t.Fatalf("package Map failed: %v %v", v, err)
	}
	// FlatMap package function
	r3 := FlatMap(Resolve[int](5), func(x int) *Promise[string] {
		return Resolve[string]("flat")
	})
	if v, err := r3.Await(); err != nil || v != "flat" {
		t.Fatalf("package FlatMap failed: %v %v", v, err)
	}
	// Catch package function
	r4 := Catch(Reject[string](errors.New("e")), func(err error) string { return "recovered" })
	if v, err := r4.Await(); err != nil || v != "recovered" {
		t.Fatalf("package Catch failed: %v %v", v, err)
	}
}

// TestAwaitTyped verifies the package-level Await returns a typed result.
func TestAwaitTyped(t *testing.T) {
	p := Resolve[int](42)
	val, err := Await(p)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if val != 42 {
		t.Errorf("expected 42, got %d", val)
	}

	// Typed zero value on error.
	ep := Reject[int](errors.New("e"))
	zero, err := Await(ep)
	if err == nil {
		t.Fatal("expected error")
	}
	if zero != 0 {
		t.Errorf("expected typed zero value 0, got %d", zero)
	}
}

// TestAllTyped verifies AllTyped resolves with a typed slice in input order.
func TestAllTyped(t *testing.T) {
	ps := []*Promise[int]{
		Async[int](func(resolve func(int), reject func(any)) {
			time.Sleep(20 * time.Millisecond)
			resolve(1)
		}),
		Async[int](func(resolve func(int), reject func(any)) {
			time.Sleep(5 * time.Millisecond)
			resolve(2)
		}),
		Resolve[int](3),
	}

	result := AllTyped(ps...)
	val, err := result.Await()
	if err != nil {
		t.Fatalf("AllTyped failed: %v", err)
	}
	if len(val) != 3 || val[0] != 1 || val[1] != 2 || val[2] != 3 {
		t.Errorf("expected [1 2 3], got %v", val)
	}
}

// TestAllTypedRejects verifies AllTyped rejects with the first error.
func TestAllTypedRejects(t *testing.T) {
	ps := []*Promise[int]{
		Resolve[int](1),
		Reject[int](errors.New("boom")),
		Resolve[int](3),
	}
	if _, err := AllTyped(ps...).Await(); err == nil {
		t.Fatal("expected AllTyped to reject")
	}
}

// TestAllTypedEmpty verifies AllTyped with no promises resolves to an empty slice.
func TestAllTypedEmpty(t *testing.T) {
	val, err := AllTyped[int]().Await()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if val == nil || len(val) != 0 {
		t.Errorf("expected empty non-nil slice, got %v", val)
	}
}

// TestEachTyped verifies EachTyped awaits each promise sequentially with a typed slice.
func TestEachTyped(t *testing.T) {
	var mu sync.Mutex
	started := make([]int, 0, 3)
	mk := func(v int, delay time.Duration) *Promise[int] {
		return Create[int](func(resolve func(int), reject func(any)) {
			mu.Lock()
			started = append(started, v)
			mu.Unlock()
			time.Sleep(delay)
			resolve(v)
		})
	}
	ps := []*Promise[int]{mk(10, 20*time.Millisecond), mk(20, 5*time.Millisecond), mk(30, 0)}

	val, err := EachTyped(ps...).Await()
	if err != nil {
		t.Fatalf("EachTyped failed: %v", err)
	}
	if len(val) != 3 || val[0] != 10 || val[1] != 20 || val[2] != 30 {
		t.Errorf("expected [10 20 30], got %v", val)
	}
	mu.Lock()
	defer mu.Unlock()
	// Lazy Create promises only start when awaited, so EachTyped must run them in order.
	if len(started) != 3 || started[0] != 10 || started[1] != 20 || started[2] != 30 {
		t.Errorf("EachTyped should await sequentially, started=%v", started)
	}
}

// TestEachTypedRejects verifies EachTyped stops on the first error.
func TestEachTypedRejects(t *testing.T) {
	ps := []*Promise[int]{
		Resolve[int](1),
		Reject[int](errors.New("stop")),
	}
	if _, err := EachTyped(ps...).Await(); err == nil {
		t.Fatal("expected EachTyped to reject")
	}
}

// TestRaceTyped verifies RaceTyped preserves the element type and picks the first settle.
func TestRaceTyped(t *testing.T) {
	ps := []*Promise[int]{
		Async[int](func(resolve func(int), reject func(any)) {
			time.Sleep(50 * time.Millisecond)
			resolve(1)
		}),
		Async[int](func(resolve func(int), reject func(any)) {
			time.Sleep(5 * time.Millisecond)
			resolve(2)
		}),
	}
	val, err := RaceTyped(ps...).Await()
	if err != nil {
		t.Fatalf("RaceTyped failed: %v", err)
	}
	if val != 2 {
		t.Errorf("expected 2 (fastest), got %d", val)
	}
}

// TestRaceTypedRejects verifies RaceTyped rejects on the first rejection.
func TestRaceTypedRejects(t *testing.T) {
	ps := []*Promise[int]{
		Reject[int](errors.New("fast-fail")),
		Async[int](func(resolve func(int), reject func(any)) {
			time.Sleep(100 * time.Millisecond)
			resolve(1)
		}),
	}
	if _, err := RaceTyped(ps...).Await(); err == nil {
		t.Fatal("expected RaceTyped to reject")
	}
}

// TestAnyConversion tests Promise[T] to Promise[any] conversion
func TestAnyConversion(t *testing.T) {
	p := Resolve[int](42)
	anyPromise := p.Any()

	val, err := anyPromise.Await()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if intVal, ok := val.(int); !ok || intVal != 42 {
		t.Errorf("expected 42, got %v", val)
	}
}

// TestToAny tests ToAny helper function
func TestToAny(t *testing.T) {
	p := Resolve[string]("hello")
	anyPromise := ToAny(p)

	val, err := anyPromise.Await()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if strVal, ok := val.(string); !ok || strVal != "hello" {
		t.Errorf("expected 'hello', got %v", val)
	}
}

// TestFromConversion tests Promise[any] to Promise[T] conversion
func TestFromConversion(t *testing.T) {
	anyPromise := Resolve[any](42)
	intPromise := From[int](anyPromise)

	val, err := intPromise.Await()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if val != 42 {
		t.Errorf("expected 42, got %d", val)
	}
}

// TestFromConversionError tests From with wrong type
func TestFromConversionError(t *testing.T) {
	anyPromise := Resolve[any]("not an int")
	intPromise := From[int](anyPromise)

	_, err := intPromise.Await()
	if err == nil {
		t.Fatal("expected type assertion error")
	}
	if err.Error() != "type assertion failed: value is not of expected type" {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestDoneOnResolvedPromise verifies Done() returns closed channel on resolved promise.
func TestDoneOnResolvedPromise(t *testing.T) {
	p := Resolve[int](42)
	_, _ = p.Await()

	select {
	case <-p.Done():
	default:
		t.Fatal("Done() channel should be closed on resolved promise")
	}
}

// TestDoneOnRejectedPromise verifies Done() returns closed channel on rejected promise.
func TestDoneOnRejectedPromise(t *testing.T) {
	p := Reject[int](errors.New("test error"))
	_, _ = p.Await()

	select {
	case <-p.Done():
	default:
		t.Fatal("Done() channel should be closed on rejected promise")
	}
}

// TestDoneBlocksUntilResolve verifies Done() blocks until promise resolves.
func TestDoneBlocksUntilResolve(t *testing.T) {
	p := Async[int](func(resolve func(int), reject func(any)) {
		time.Sleep(10 * time.Millisecond)
		resolve(42)
	})

	select {
	case <-p.Done():
		val, err := p.Await()
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if val != 42 {
			t.Errorf("expected 42, got %d", val)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Done()")
	}
}

// TestDoneInSelect verifies Done() works in select with other channels.
func TestDoneInSelect(t *testing.T) {
	p := Async[int](func(resolve func(int), reject func(any)) {
		time.Sleep(10 * time.Millisecond)
		resolve(99)
	})

	cancel := make(chan struct{})
	select {
	case <-p.Done():
		val, _ := p.Await()
		if val != 99 {
			t.Errorf("expected 99, got %d", val)
		}
	case <-cancel:
		t.Fatal("unexpected cancellation")
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
}

// TestDoneRespectsCancel verifies Done() in a select with ctx cancellation wins.
func TestDoneRespectsCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := Async[int](func(resolve func(int), reject func(any)) {
		time.Sleep(10 * time.Millisecond)
		resolve(42)
	})

	cancel() // cancel before promise resolves

	select {
	case <-ctx.Done():
		// expected — cancellation wins
	case <-p.Done():
		t.Fatal("expected cancellation to win, but promise resolved first")
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
}

// TestChainedTransformations tests chaining multiple transformations via methods.
func TestChainedTransformations(t *testing.T) {
	p := Resolve[int](10)

	result := p.
		Then(func(x int) int { return x * 2 }).
		Map(func(x int) float64 { return float64(x) + 0.5 }).
		Then(func(x float64) string { return "result" })

	val, err := result.Await()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if val != "result" {
		t.Errorf("expected 'result', got %q", val)
	}
}
