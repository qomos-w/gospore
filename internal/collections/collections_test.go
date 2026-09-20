package collections

import (
	"slices"
	"testing"
)

func TestSet(t *testing.T) {
	s := NewSet[string](2)
	if s.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", s.Len())
	}
	if !s.Add("a") {
		t.Fatal("Add(\"a\") = false, want true")
	}
	if s.Add("a") {
		t.Fatal("Add(\"a\") twice = true, want false")
	}
	s.Add("b")
	if !s.Has("a") || !s.Has("b") || s.Has("c") {
		t.Fatal("Has mismatch after adding a,b")
	}
	if got := s.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
	vals := s.Values()
	slices.Sort(vals)
	if !slices.Equal(vals, []string{"a", "b"}) {
		t.Fatalf("Values() = %v, want [a b]", vals)
	}
	s.Delete("a")
	if s.Has("a") || s.Len() != 1 {
		t.Fatal("Delete did not remove \"a\"")
	}
	s.Delete("missing")
	if s.Len() != 1 {
		t.Fatal("Delete of missing value changed Len")
	}
}

func TestSortedKeys(t *testing.T) {
	m := map[string]int{"c": 3, "a": 1, "b": 2}
	if got := SortedKeys(m); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("SortedKeys() = %v, want [a b c]", got)
	}
	if got := SortedKeys(map[int]string{3: "c", 1: "a"}); !slices.Equal(got, []int{1, 3}) {
		t.Fatalf("SortedKeys() = %v, want [1 3]", got)
	}
	if got := SortedKeys(map[string]int{}); len(got) != 0 {
		t.Fatalf("SortedKeys(empty) = %v, want empty", got)
	}
}
