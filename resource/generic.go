package resource

import "fmt"

// Key is a typed resource key. Name is for diagnostics / logs only; it
// does NOT participate in lookup — Key{Name:"x"} and Key{Name:"x"}
// declared in different packages are different keys (Go map equality).
//
// Recommended usage: declare each Key as a package-level var so all
// callers share the same value:
//
//	var DBKey = resource.NewKey[*sql.DB]("db")
type Key[T any] struct{ Name string }

// NewKey constructs a typed Key with the given diagnostic name.
func NewKey[T any](name string) Key[T] {
	return Key[T]{Name: name}
}

// Set binds v to k in r. Returns ErrFrozen after freeze.
func Set[T any](r Registry, k Key[T], v T) error {
	return r.Set(k, v)
}

// Get retrieves the value bound to k. Returns (zero, false) on miss
// or on type mismatch (the latter is a programmer error; callers
// should treat the returned ok==false the same as a miss and surface
// DiagTypeMismatch via their own logger if useful).
func Get[T any](r Registry, k Key[T]) (T, bool) {
	var zero T
	raw, ok := r.Get(k)
	if !ok {
		return zero, false
	}
	typed, ok := raw.(T)
	if !ok {
		return zero, false
	}
	return typed, true
}

// MustGet is like Get but panics on miss or type mismatch. Use only
// for keys this package owns and is certain to set.
func MustGet[T any](r Registry, k Key[T]) T {
	raw, ok := r.Get(k)
	if !ok {
		panic(fmt.Errorf("%s: key %q not registered", DiagUnknown, k.Name))
	}
	typed, ok := raw.(T)
	if !ok {
		var zero T
		panic(fmt.Errorf("%s: key %q stored as %T, requested as %T",
			DiagTypeMismatch, k.Name, raw, zero))
	}
	return typed
}
