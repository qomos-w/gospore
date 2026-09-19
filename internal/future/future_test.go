package future

import (
	"errors"
	"testing"
	"time"
)

func TestFuture_Resolve(t *testing.T) {
	f := New[int]()

	f.Resolve(42)

	select {
	case <-f.Done():
	default:
		t.Fatal("Done() should be closed after Resolve")
	}

	v, err := f.Get()
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if v != 42 {
		t.Errorf("Get = %d, want 42", v)
	}
}

func TestFuture_Reject(t *testing.T) {
	f := New[string]()
	wantErr := errors.New("boom")

	f.Reject(wantErr)

	select {
	case <-f.Done():
	default:
		t.Fatal("Done() should be closed after Reject")
	}

	v, err := f.Get()
	if err != wantErr {
		t.Fatalf("Get error = %v, want %v", err, wantErr)
	}
	if v != "" {
		t.Errorf("Get value = %q, want zero", v)
	}
}

func TestFuture_DoubleResolveNoOp(t *testing.T) {
	f := New[int]()

	f.Resolve(1)
	f.Resolve(2) // second call should be no-op

	v, _ := f.Get()
	if v != 1 {
		t.Errorf("Get = %d, want 1 (first resolve wins)", v)
	}
}

func TestFuture_DoubleRejectNoOp(t *testing.T) {
	f := New[int]()
	e1 := errors.New("first")
	e2 := errors.New("second")

	f.Reject(e1)
	f.Reject(e2) // second call should be no-op

	_, err := f.Get()
	if err != e1 {
		t.Errorf("Get error = %v, want %v (first reject wins)", err, e1)
	}
}

func TestFuture_ResolveThenReject(t *testing.T) {
	f := New[int]()

	f.Resolve(10)
	f.Reject(errors.New("late")) // no-op

	v, err := f.Get()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != 10 {
		t.Errorf("Get = %d, want 10", v)
	}
}

func TestFuture_BlocksUntilResolved(t *testing.T) {
	f := New[int]()

	done := make(chan struct{})
	go func() {
		f.Get()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Get should block until Resolve")
	case <-time.After(50 * time.Millisecond):
	}

	f.Resolve(99)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Get did not unblock after Resolve")
	}
}

func TestFuture_GenericTypes(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		f := New[string]()
		f.Resolve("hello")
		v, _ := f.Get()
		if v != "hello" {
			t.Errorf("got %q, want \"hello\"", v)
		}
	})

	t.Run("struct", func(t *testing.T) {
		type point struct{ X, Y int }
		f := New[point]()
		f.Resolve(point{3, 4})
		v, _ := f.Get()
		if v.X != 3 || v.Y != 4 {
			t.Errorf("got %+v, want {3 4}", v)
		}
	})
}
