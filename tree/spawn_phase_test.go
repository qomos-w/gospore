package tree

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/ref"
)

// phaseFixture builds a Tree whose Build phase is externally gated per
// child name, so tests can hold a spawn mid-build and observe the
// pending window.
type phaseFixture struct {
	tr  Tree
	mu  sync.Mutex
	raw byte
	// gates maps child name to a channel Build blocks on until closed.
	gates     map[string]chan struct{}
	termIDs   []id.ActorID
	abortIDs  []id.ActorID
	reserved  map[string]ref.Ref
	buildErrs map[string]error
	// onBuild, when set, replaces the canned error result of Build.
	onBuild func(name string, parent ref.Ref, reserved ref.Ref) error
}

func newPhaseFixture(t *testing.T) *phaseFixture {
	t.Helper()
	f := &phaseFixture{
		gates:     map[string]chan struct{}{},
		reserved:  map[string]ref.Ref{},
		buildErrs: map[string]error{},
		raw:       0x10,
	}
	root := newMockRef(0x01)
	tr, err := New(Config{
		Root: root,
		Allocator: &AllocatorFuncs{
			ReserveFn: func(parent ref.Ref, props actor.Props, name string) (ref.Ref, error) {
				f.mu.Lock()
				defer f.mu.Unlock()
				f.raw++
				r := newMockRef(f.raw)
				f.reserved[name] = r
				return r, nil
			},
			BuildFn: func(parent ref.Ref, props actor.Props, name string, reserved ref.Ref) error {
				f.mu.Lock()
				gate := f.gates[name]
				err := f.buildErrs[name]
				hook := f.onBuild
				f.mu.Unlock()
				if gate != nil {
					<-gate
				}
				if hook != nil {
					return hook(name, parent, reserved)
				}
				return err
			},
			AbortFn: func(reserved ref.Ref) {
				f.mu.Lock()
				defer f.mu.Unlock()
				f.abortIDs = append(f.abortIDs, reserved.ID())
			},
		},
		Idler:      func(ref.Ref) error { return nil },
		Terminator: func(r ref.Ref) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.termIDs = append(f.termIDs, r.ID())
			return nil
		},
	})
	if err != nil {
		t.Fatalf("tree.New: %v", err)
	}
	f.tr = tr
	return f
}

// holdBuild makes Build block for the named child until release.
func (f *phaseFixture) holdBuild(name string) (release func()) {
	f.mu.Lock()
	ch := make(chan struct{})
	f.gates[name] = ch
	f.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { close(ch) }) }
}

// failBuild makes Build return err for the named child. A nil err clears
// a previously injected failure so retries can succeed.
func (f *phaseFixture) failBuild(name string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.buildErrs[name] = err
}

// spawnAsync runs Spawn in the background and returns its result channel.
func (f *phaseFixture) spawnAsync(t *testing.T, parent ref.Ref, name string) <-chan error {
	t.Helper()
	out := make(chan error, 1)
	go func() {
		_, err := f.tr.Spawn(parent, actor.Props{}, name)
		out <- err
	}()
	return out
}

// waitReserved polls until Reserve has recorded the name, proving Phase
// 1 completed and Build is in flight. Must not probe via Spawn: a probe
// that races ahead of the real reservation would itself enter Build and
// block on the gate.
func (f *phaseFixture) waitReserved(t *testing.T, name string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f.reservedRef(name) != nil {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("name %q never reserved", name)
}

func (f *phaseFixture) reservedRef(name string) ref.Ref {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reserved[name]
}

func (f *phaseFixture) terminatedIDs() []id.ActorID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]id.ActorID(nil), f.termIDs...)
}

func (f *phaseFixture) abortedIDs() []id.ActorID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]id.ActorID(nil), f.abortIDs...)
}

func rootOf(f *phaseFixture) ref.Ref { return f.tr.Root() }

// A slow Build must not block an unrelated concurrent Spawn: the write
// lock is only held for reservation, never during Build.
func TestTree_SpawnSlowBuildDoesNotBlockConcurrentSpawn(t *testing.T) {
	f := newPhaseFixture(t)
	root := rootOf(f)
	release := f.holdBuild("slow")

	slowCh := f.spawnAsync(t, root, "slow")
	f.waitReserved(t, "slow")

	done := make(chan error, 1)
	go func() {
		_, err := f.tr.Spawn(root, actor.Props{}, "fast")
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("fast Spawn blocked by slow Build or failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fast Spawn stalled behind slow Build — write lock held during Build")
	}

	release()
	select {
	case err := <-slowCh:
		if err != nil {
			t.Fatalf("slow Spawn: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("slow Spawn never completed after release")
	}
	if got := len(f.tr.Children(root)); got != 2 {
		t.Errorf("Children after both spawns: got %d, want 2", got)
	}
}

// Until commit, a pending node is invisible to Walk/Children/Parent/
// LookupID/Stop, but its name slot and ActorID are already reserved.
func TestTree_SpawnPendingInvisibleUntilCommit(t *testing.T) {
	f := newPhaseFixture(t)
	root := rootOf(f)
	release := f.holdBuild("alice")

	aliceCh := f.spawnAsync(t, root, "alice")
	f.waitReserved(t, "alice")
	alice := f.reservedRef("alice")
	if _, ok := f.tr.LookupID(alice.ID()); ok {
		t.Error("LookupID must not expose a pending node")
	}
	if got := f.tr.Children(root); len(got) != 0 {
		t.Errorf("Children during pending: got %d, want 0", len(got))
	}
	walked := 0
	f.tr.Walk(func(r ref.Ref) bool {
		if r.ID() == alice.ID() {
			walked++
		}
		return true
	})
	if walked != 0 {
		t.Error("Walk must skip pending nodes")
	}
	if _, ok := f.tr.Parent(alice); ok {
		t.Error("Parent must not expose a pending node")
	}
	if err := f.tr.Stop(alice); err == nil {
		t.Error("Stop must reject a pending node")
	}

	release()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := f.tr.LookupID(alice.ID()); ok {
			if err := <-aliceCh; err != nil {
				t.Fatalf("alice Spawn: %v", err)
			}
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("committed child never became visible via LookupID")
}

// A Build error rolls the reservation back: nothing stays registered and
// the name becomes available again.
func TestTree_SpawnBuildErrorRollsBackReservation(t *testing.T) {
	f := newPhaseFixture(t)
	root := rootOf(f)
	sentinel := errors.New("build boom")
	f.failBuild("alice", sentinel)

	if _, err := f.tr.Spawn(root, actor.Props{}, "alice"); !errors.Is(err, sentinel) {
		t.Fatalf("Spawn err: got %v, want sentinel", err)
	}
	if got := f.tr.Children(root); len(got) != 0 {
		t.Errorf("Children after failed build: got %d, want 0", len(got))
	}
	alice := f.reservedRef("alice")
	if _, ok := f.tr.LookupID(alice.ID()); ok {
		t.Error("LookupID must not expose a rolled-back node")
	}

	// The name is free again and a retry succeeds.
	f.failBuild("alice", nil)
	if _, err := f.tr.Spawn(root, actor.Props{}, "alice"); err != nil {
		t.Fatalf("retry Spawn after rollback: %v", err)
	}
	if got := len(f.tr.Children(root)); got != 1 {
		t.Errorf("Children after retry: got %d, want 1", got)
	}
	if aborts := f.abortedIDs(); len(aborts) != 1 || aborts[0] != alice.ID() {
		t.Errorf("Abort calls: got %v, want exactly [alice]", aborts)
	}
}

// Destroying the parent while a child is mid-build must roll the spawn
// back: Spawn fails, the pending subtree is swept, nothing leaks.
func TestTree_SpawnParentDestroyedDuringBuild(t *testing.T) {
	f := newPhaseFixture(t)
	root := rootOf(f)

	mid, err := f.tr.Spawn(root, actor.Props{}, "mid")
	if err != nil {
		t.Fatalf("Spawn mid: %v", err)
	}
	release := f.holdBuild("kid")
	kidCh := f.spawnAsync(t, mid, "kid")
	f.waitReserved(t, "kid")
	kid := f.reservedRef("kid")

	if err := f.tr.Destroy(mid); err != nil {
		t.Fatalf("Destroy mid during kid build: %v", err)
	}

	release()
	select {
	case err := <-kidCh:
		if !errors.Is(err, ErrUnknownParent) {
			t.Fatalf("kid Spawn err: got %v, want ErrUnknownParent", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("kid Spawn never returned after parent destroyed")
	}

	terminated := f.terminatedIDs()
	sawMid, sawKid := false, false
	for _, idv := range terminated {
		if idv == mid.ID() {
			sawMid = true
		}
		if idv == kid.ID() {
			sawKid = true
		}
	}
	if !sawMid || !sawKid {
		t.Errorf("terminator IDs after parent destroy: got %v, want both mid and kid swept", terminated)
	}
	if _, ok := f.tr.LookupID(kid.ID()); ok {
		t.Error("kid must not remain in byID after rollback")
	}
	if _, ok := f.tr.LookupID(mid.ID()); ok {
		t.Error("mid must not remain in byID after destroy")
	}
}

// A Build that re-enters Spawn (a child spawning a grandchild during its
// own construction) must not deadlock against the spawner — the
// historical lock-during-allocate shape deadlocked here.
func TestTree_SpawnGrandchildInsideBuildNoDeadlock(t *testing.T) {
	f := newPhaseFixture(t)
	root := rootOf(f)

	f.mu.Lock()
	f.onBuild = func(name string, parent ref.Ref, reserved ref.Ref) error {
		if name != "parent" {
			return nil
		}
		// Re-enter the tree from inside Build, synchronously.
		if _, err := f.tr.Spawn(reserved, actor.Props{}, "grand"); err != nil {
			return err
		}
		return nil
	}
	f.mu.Unlock()

	parentRef, err := f.tr.Spawn(root, actor.Props{}, "parent")
	if err != nil {
		t.Fatalf("Spawn parent: %v", err)
	}
	if got := f.tr.Children(parentRef); len(got) != 1 {
		t.Errorf("grandchild not attached: Children = %d, want 1", len(got))
	}
	if _, ok := f.tr.Parent(f.reservedRef("grand")); !ok {
		t.Error("grandchild must be committable under its parent")
	}
}
