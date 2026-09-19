package plan

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/qomos-w/gospore/ref"
)

func TestState_String(t *testing.T) {
	cases := []struct {
		s    State
		want string
	}{
		{StatePending, "pending"},
		{StateRunning, "running"},
		{StateCompleted, "completed"},
		{StateFailed, "failed"},
		{StateCancelled, "cancelled"},
		{State(99), ""},
	}
	for _, c := range cases {
		if got := c.s.String(); got != c.want {
			t.Errorf("State(%d).String() = %q, want %q", c.s, got, c.want)
		}
	}
}

func TestState_IsTerminal(t *testing.T) {
	terminal := []State{StateCompleted, StateFailed, StateCancelled}
	for _, s := range terminal {
		if !s.IsTerminal() {
			t.Errorf("State(%d) should be terminal", s)
		}
	}
	nonTerminal := []State{StatePending, StateRunning}
	for _, s := range nonTerminal {
		if s.IsTerminal() {
			t.Errorf("State(%d) should not be terminal", s)
		}
	}
	if State(99).IsTerminal() {
		t.Error("unknown state should not be terminal")
	}
}

func TestState_CanStart(t *testing.T) {
	if !StatePending.CanStart() {
		t.Error("StatePending.CanStart() should be true")
	}
	for _, s := range []State{StateRunning, StateCompleted, StateFailed, StateCancelled} {
		if s.CanStart() {
			t.Errorf("State(%d).CanStart() should be false", s)
		}
	}
}

func TestNewConfig_Defaults(t *testing.T) {
	cfg := NewConfig()
	if cfg.Name != "" {
		t.Errorf("Name = %q, want zero", cfg.Name)
	}
	if cfg.AutoStart {
		t.Error("AutoStart should be false by default")
	}
	if cfg.Timeout != 0 {
		t.Errorf("Timeout = %v, want 0", cfg.Timeout)
	}
	if cfg.KeepAfterDone {
		t.Error("KeepAfterDone should be false by default")
	}
}

func TestNewConfig_WithName(t *testing.T) {
	cfg := NewConfig(WithName("my-plan"))
	if cfg.Name != "my-plan" {
		t.Errorf("Name = %q, want \"my-plan\"", cfg.Name)
	}
}

func TestNewConfig_WithAutoStart(t *testing.T) {
	cfg := NewConfig(WithAutoStart())
	if !cfg.AutoStart {
		t.Error("AutoStart should be true")
	}
}

func TestNewConfig_WithTimeout(t *testing.T) {
	cfg := NewConfig(WithTimeout(5 * time.Second))
	if cfg.Timeout != 5*time.Second {
		t.Errorf("Timeout = %v, want 5s", cfg.Timeout)
	}
}

func TestNewConfig_WithKeepAfterDone(t *testing.T) {
	cfg := NewConfig(WithKeepAfterDone())
	if !cfg.KeepAfterDone {
		t.Error("KeepAfterDone should be true")
	}
}

func TestNewConfig_WithRetry(t *testing.T) {
	cfg := NewConfig(WithRetry(3, 100*time.Millisecond))
	if cfg.RetryMax != 3 {
		t.Errorf("RetryMax = %d, want 3", cfg.RetryMax)
	}
	if cfg.RetryBackoff != 100*time.Millisecond {
		t.Errorf("RetryBackoff = %v, want 100ms", cfg.RetryBackoff)
	}
}

func TestNewConfig_WithCircuitBreaker(t *testing.T) {
	cfg := NewConfig(WithCircuitBreaker(5, time.Minute))
	if cfg.CircuitThreshold != 5 {
		t.Errorf("CircuitThreshold = %d, want 5", cfg.CircuitThreshold)
	}
	if cfg.CircuitWindow != time.Minute {
		t.Errorf("CircuitWindow = %v, want 1m", cfg.CircuitWindow)
	}
}

func TestNewConfig_WithPause(t *testing.T) {
	cfg := NewConfig(WithPause())
	if !cfg.PauseOnCreate {
		t.Error("PauseOnCreate should be true")
	}
}

func TestNewConfig_NilOption(t *testing.T) {
	// nil Option should be silently ignored
	cfg := NewConfig(nil, WithName("x"), nil)
	if cfg.Name != "x" {
		t.Errorf("Name = %q, want \"x\"", cfg.Name)
	}
}

func TestNewConfig_Composed(t *testing.T) {
	cfg := NewConfig(
		WithName("composed"),
		WithAutoStart(),
		WithTimeout(3*time.Second),
		WithKeepAfterDone(),
	)
	if cfg.Name != "composed" {
		t.Errorf("Name = %q", cfg.Name)
	}
	if !cfg.AutoStart {
		t.Error("AutoStart should be true")
	}
	if cfg.Timeout != 3*time.Second {
		t.Errorf("Timeout = %v", cfg.Timeout)
	}
	if !cfg.KeepAfterDone {
		t.Error("KeepAfterDone should be true")
	}
}

func TestDiagConstants(t *testing.T) {
	if DiagInvalidTarget != "gospore.plan.invalid_target" {
		t.Errorf("DiagInvalidTarget = %q", DiagInvalidTarget)
	}
	if DiagStartWrongState != "gospore.plan.start_wrong_state" {
		t.Errorf("DiagStartWrongState = %q", DiagStartWrongState)
	}
	if DiagTimeout != "gospore.plan.timeout" {
		t.Errorf("DiagTimeout = %q", DiagTimeout)
	}
	if DiagStreamingResultUnavailable != "gospore.plan.streaming_result_unavailable" {
		t.Errorf("DiagStreamingResultUnavailable = %q", DiagStreamingResultUnavailable)
	}
}

func TestMapErrToDiag(t *testing.T) {
	if got := MapErrToDiag(nil); got != "" {
		t.Errorf("nil → %q", got)
	}
	if got := MapErrToDiag(ErrStreamingResultUnavailable); got != DiagStreamingResultUnavailable {
		t.Errorf("sentinel → %q, want %q", got, DiagStreamingResultUnavailable)
	}
	if got := MapErrToDiag(errors.New("other")); got != "" {
		t.Errorf("other error → %q, want \"\"", got)
	}
}

type recvChanNode struct {
	results []RecvResult
	idx     int
}

func (n *recvChanNode) Ref() ref.Ref                { return nil }
func (n *recvChanNode) State() State                { return StateRunning }
func (n *recvChanNode) Start(context.Context) error { return nil }
func (n *recvChanNode) Stop() error                 { return nil }
func (n *recvChanNode) Result() (any, error)        { return nil, nil }
func (n *recvChanNode) Done() <-chan struct{}       { return nil }
func (n *recvChanNode) Target() ref.Ref             { return nil }
func (n *recvChanNode) CallID() string              { return "" }
func (n *recvChanNode) Payload() any                { return nil }
func (n *recvChanNode) Recv() (any, error) {
	if n.idx >= len(n.results) {
		return nil, io.EOF
	}
	r := n.results[n.idx]
	n.idx++
	return r.Value, r.Err
}

type blockingRecvChanNode struct {
	release chan struct{}
}

func (n *blockingRecvChanNode) Ref() ref.Ref                { return nil }
func (n *blockingRecvChanNode) State() State                { return StateRunning }
func (n *blockingRecvChanNode) Start(context.Context) error { return nil }
func (n *blockingRecvChanNode) Stop() error                 { return nil }
func (n *blockingRecvChanNode) Result() (any, error)        { return nil, nil }
func (n *blockingRecvChanNode) Done() <-chan struct{}       { return nil }
func (n *blockingRecvChanNode) Target() ref.Ref             { return nil }
func (n *blockingRecvChanNode) CallID() string              { return "" }
func (n *blockingRecvChanNode) Payload() any                { return nil }
func (n *blockingRecvChanNode) Recv() (any, error) {
	<-n.release
	return "late", nil
}

func TestRecvChan_StreamsValuesAndEOF(t *testing.T) {
	node := &recvChanNode{results: []RecvResult{{Value: "hello"}, {Err: io.EOF}}}
	ch := RecvChan(context.Background(), node)

	first, ok := <-ch
	if !ok {
		t.Fatal("first recv: channel closed")
	}
	if first.Value != "hello" || first.Err != nil {
		t.Fatalf("first recv = %+v, want hello/nil", first)
	}

	second, ok := <-ch
	if !ok {
		t.Fatal("second recv: channel closed")
	}
	if !errors.Is(second.Err, io.EOF) {
		t.Fatalf("second err = %v, want EOF", second.Err)
	}

	_, ok = <-ch
	if ok {
		t.Fatal("expected channel to close after terminal recv")
	}
}

func TestRecvChan_DropsDeliveryAfterContextCancel(t *testing.T) {
	node := &blockingRecvChanNode{release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	ch := RecvChan(ctx, node)
	cancel()
	close(node.release)

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to close without delivering cancelled recv")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for RecvChan to close")
	}
}
