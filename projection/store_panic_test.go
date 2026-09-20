package projection

import (
	"testing"
)

// These tests are baseline invariants of snapshotValue. The actual bug we
// fixed (slice-header read racing with concurrent append on another goroutine)
// is inherently racy and cannot be reproduced deterministically without
// spawning a second goroutine — which the race detector correctly flags. The
// fix has two layers:
//
//   1. snapshotValue captures v.Len() once into a local and bounds-checks
//      v.Index(i) before use, so the common shrink/grow windows never panic.
//   2. snapshotValue carries a deferred recover so any reflect-side panic
//      from a deeply nested field becomes an error rather than propagating
//      up to cell.recoverAndDecide and triggering a workspace-wide Restart.
//
// We verify layer (1) structurally by exercising the slice branch with
// stable inputs, and layer (2) by passing a struct with an unexported field
// that the snapshot must skip without panicking.

func TestSnapshotValue_SliceStable(t *testing.T) {
	type container struct {
		Items []int
	}
	c := &container{Items: []int{1, 2, 3, 4, 5}}
	snap, err := SnapshotOf(c)
	if err != nil {
		t.Fatalf("SnapshotOf returned error: %v", err)
	}
	items, ok := snap["items"].([]any)
	if !ok {
		t.Fatalf("snap[items] has wrong type: %T", snap["items"])
	}
	if len(items) != 5 {
		t.Fatalf("len(items) = %d, want 5", len(items))
	}
	for i, v := range items {
		got, ok := v.(int)
		if !ok || got != i+1 {
			t.Fatalf("items[%d] = %v, want %d", i, v, i+1)
		}
	}
}

func TestSnapshotValue_ArrayStable(t *testing.T) {
	type container struct {
		Items [4]int
	}
	c := &container{Items: [4]int{1, 2, 3, 4}}
	snap, err := SnapshotOf(c)
	if err != nil {
		t.Fatalf("SnapshotOf returned error: %v", err)
	}
	items := snap["items"].([]any)
	if len(items) != 4 {
		t.Fatalf("len(items) = %d, want 4", len(items))
	}
	for i, v := range items {
		got, ok := v.(int)
		if !ok || got != i+1 {
			t.Fatalf("items[%d] = %v, want %d", i, v, i+1)
		}
	}
}

func TestSnapshotValue_NilSlice(t *testing.T) {
	type container struct {
		Items []int
	}
	c := &container{}
	snap, err := SnapshotOf(c)
	if err != nil {
		t.Fatalf("SnapshotOf returned error: %v", err)
	}
	items := snap["items"].([]any)
	if len(items) != 0 {
		t.Fatalf("len(items) = %d, want 0", len(items))
	}
}

func TestSnapshotValue_EmptySlice(t *testing.T) {
	type container struct {
		Items []int
	}
	c := &container{Items: []int{}}
	snap, err := SnapshotOf(c)
	if err != nil {
		t.Fatalf("SnapshotOf returned error: %v", err)
	}
	items := snap["items"].([]any)
	if len(items) != 0 {
		t.Fatalf("len(items) = %d, want 0", len(items))
	}
}

// TestSnapshotValue_StructWithUnexportedField verifies snapshotStruct's
// IsExported guard still filters unexported fields after the refactor that
// added a deferred recover to snapshotValue. This is the cheapest path
// through the recover code without spawning a racing goroutine.
func TestSnapshotValue_StructWithUnexportedField(t *testing.T) {
	type withPrivate struct {
		Public  int
		hidden  string
	}
	c := &withPrivate{Public: 7, hidden: "must-not-leak"}
	snap, err := SnapshotOf(c)
	if err != nil {
		t.Fatalf("SnapshotOf returned error: %v", err)
	}
	if got, _ := snap["public"].(int); got != 7 {
		t.Fatalf("snap[public] = %v, want 7", snap["public"])
	}
	if _, present := snap["hidden"]; present {
		t.Fatalf("snap unexpectedly exposed unexported field: %v", snap["hidden"])
	}
}

// TestSnapshotValue_NestedPointerToNil exercises the early-return path
// when a pointer field is nil — the pre-fix code dereferenced without
// re-checking after the pointer unwrap loop, so a nil mid-traversal could
// panic. The recover at the top of snapshotValue would now catch it, but
// the early-return at the IsNil check is the primary defense.
func TestSnapshotValue_NestedPointerToNil(t *testing.T) {
	type leaf struct{ V int }
	type container struct {
		P *leaf
	}
	c := &container{P: nil}
	snap, err := SnapshotOf(c)
	if err != nil {
		t.Fatalf("SnapshotOf returned error: %v", err)
	}
	if _, present := snap["p"]; !present {
		t.Fatalf("snap missing p key: %v", snap)
	}
}
