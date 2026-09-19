// Package future is gospore's internal single-resolve Future
// primitive. It is the bridge between Cell-side completion (one
// Resolve / Reject call from the producing goroutine) and
// caller-side waiting (Done / Get from any consumer goroutine).
//
// Listed under ARCHITECTURE.md §8.3 as an internal implementation
// detail — callers outside the gospore tree must not import it.
package future

import "sync"

// Future is a one-shot, generic resolve-or-reject value. After the
// first Resolve or Reject every subsequent call is a no-op; the
// stored result is immutable. Done is closed exactly once on
// completion.
type Future[T any] interface {
	// Done returns a channel closed when the Future is resolved or
	// rejected.
	Done() <-chan struct{}
	// Get blocks until completion and returns the stored value or
	// error.
	Get() (T, error)
	// Resolve stores value and closes Done. No-op on a completed
	// Future.
	Resolve(value T)
	// Reject stores err and closes Done. No-op on a completed
	// Future.
	Reject(err error)
}

// New constructs an unresolved Future[T].
func New[T any]() Future[T] {
	return &future[T]{done: make(chan struct{})}
}

type future[T any] struct {
	done chan struct{}
	once sync.Once
	val  T
	err  error
}

func (f *future[T]) Done() <-chan struct{} {
	return f.done
}

func (f *future[T]) Get() (T, error) {
	<-f.done
	return f.val, f.err
}

func (f *future[T]) Resolve(value T) {
	f.once.Do(func() {
		f.val = value
		close(f.done)
	})
}

func (f *future[T]) Reject(err error) {
	f.once.Do(func() {
		f.err = err
		close(f.done)
	})
}
