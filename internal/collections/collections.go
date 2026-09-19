// Package collections provides small generic collection helpers shared
// across gospore internals: a comparable-keyed Set and deterministic
// map-key ordering.
package collections

import (
	"cmp"
	"slices"
)

// Set is a set of comparable values. The zero value is not usable;
// construct via NewSet.
type Set[T comparable] struct {
	m map[T]struct{}
}

// NewSet returns an empty Set pre-sized for capacity entries.
func NewSet[T comparable](capacity int) *Set[T] {
	return &Set[T]{m: make(map[T]struct{}, capacity)}
}

// Add inserts v and reports whether v was absent before.
func (s *Set[T]) Add(v T) bool {
	if _, ok := s.m[v]; ok {
		return false
	}
	s.m[v] = struct{}{}
	return true
}

// Has reports whether v is in the set.
func (s *Set[T]) Has(v T) bool {
	_, ok := s.m[v]
	return ok
}

// Delete removes v if present.
func (s *Set[T]) Delete(v T) {
	delete(s.m, v)
}

// Len reports the number of values in the set.
func (s *Set[T]) Len() int {
	return len(s.m)
}

// Values returns the set's values. The order is unspecified.
func (s *Set[T]) Values() []T {
	out := make([]T, 0, len(s.m))
	for v := range s.m {
		out = append(out, v)
	}
	return out
}

// SortedKeys returns the keys of m in ascending order.
func SortedKeys[K cmp.Ordered, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
