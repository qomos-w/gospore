package cell

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/plan"
	"github.com/qomos-w/gospore/projection"
	"github.com/qomos-w/gospore/ref"
)

// planNodeActor implements both actor.Actor and plan.Node.
// It is spawned as a child of the caller actor and wraps a single
// Invoke so that it is observable (Projection), controllable
// (Stop / Timeout), and supervised like any other actor.
type planNodeActor struct {
	actor.Host
	Comp plan.PlanComponent // exported for SnapshotOf projection

	cfg       planNodeConfig
	mu        sync.Mutex
	done      chan struct{}
	recvCh    chan recvResult
	call      *invoke.Call
	projVer   uint64
	selfRef   ref.Ref
	runCancel context.CancelFunc // cancels the running stream goroutine
	closeDone sync.Once          // guards close(p.done)
}

type planNodeConfig struct {
	target        ref.Ref
	callID        string
	payload       any
	autoStart     bool
	timeout       time.Duration
	keepAfterDone bool
	name          string
	projections   *projection.StoreImpl
}

type recvResult struct {
	value any
	err   error
}

func newPlanNodeActor(cfg planNodeConfig) *planNodeActor {
	return &planNodeActor{
		cfg:    cfg,
		done:   make(chan struct{}),
		recvCh: make(chan recvResult, 16),
	}
}

func (p *planNodeActor) setSelfRef(r ref.Ref) {
	p.mu.Lock()
	p.selfRef = r
	p.mu.Unlock()
}

func (p *planNodeActor) Props() actor.Props {
	return actor.PropsFromFunc(func() actor.Actor {
		return p
	})
}

func (p *planNodeActor) Type() string { return "plan" }

// OnStart initializes the PlanComponent and optionally auto-starts
// the wrapped Invoke.
func (p *planNodeActor) OnStart(ctx actor.Context) error {
	p.mu.Lock()
	p.selfRef = ctx.Self()

	// Re-initialize channels on restart (done was closed by prior OnStop).
	select {
	case <-p.done:
		p.done = make(chan struct{})
		p.recvCh = make(chan recvResult, 16)
		p.call = nil
		p.runCancel = nil
		p.closeDone = sync.Once{}
	default:
	}

	p.Comp = plan.PlanComponent{
		State:    plan.StatePending,
		CallID:   p.cfg.callID,
		CallerID: ctx.Parent().ID(),
		TargetID: p.cfg.target.ID(),
	}
	p.mu.Unlock()

	p.publishProjection()

	if p.cfg.autoStart {
		return p.Start(ctx.Lifecycle())
	}
	return nil
}

// OnStop transitions to Cancelled if not already terminal,
// waits for the stream goroutine to finish (unless keepAfterDone).
// Destroy is the caller's responsibility (Call/Stream handle it
// automatically; Plan users must call ctx.Destroy explicitly).
func (p *planNodeActor) OnStop(ctx actor.Context) error {
	_ = p.Stop()
	if !p.cfg.keepAfterDone {
		<-p.done
	}
	p.pruneProjection()
	return nil
}

// pruneProjection removes the node's projection entry. Plan nodes
// are ephemeral (one per invoke, unique actor ID) and their teardown
// paths use both Stop and Destroy, so OnStop is the only hook that
// always fires; without pruning, every completed invoke leaves a
// snapshot in the projection store for the process lifetime. A
// restarted node republishes in OnStart.
func (p *planNodeActor) pruneProjection() {
	p.mu.Lock()
	proj := p.cfg.projections
	self := p.selfRef
	p.mu.Unlock()
	if proj != nil && self != nil {
		proj.Delete(self.ID())
	}
}

// --- plan.Node interface ---

func (p *planNodeActor) Ref() ref.Ref {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.selfRef
}

func (p *planNodeActor) State() plan.State {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Comp.State
}

func (p *planNodeActor) Target() ref.Ref       { return p.cfg.target }
func (p *planNodeActor) CallID() string        { return p.cfg.callID }
func (p *planNodeActor) Payload() any          { return p.cfg.payload }
func (p *planNodeActor) Done() <-chan struct{} { return p.done }

// Start transitions Pending -> Running and launches the Invoke.
func (p *planNodeActor) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.Comp.State != plan.StatePending {
		p.mu.Unlock()
		return fmt.Errorf(plan.DiagStartWrongState)
	}
	p.Comp.State = plan.StateRunning
	p.Comp.StartedAt = p.now()
	ctx, cancel := context.WithCancel(ctx)
	p.runCancel = cancel
	p.mu.Unlock()

	p.publishProjection()

	if p.cfg.timeout > 0 {
		go p.runWithTimeout(ctx)
	} else {
		go p.runStream(ctx)
	}
	return nil
}

// Stop transitions any non-terminal state -> Cancelled.
func (p *planNodeActor) Stop() error {
	p.mu.Lock()
	if p.Comp.State.IsTerminal() {
		p.mu.Unlock()
		return nil
	}
	p.Comp.State = plan.StateCancelled
	p.Comp.ErrorMsg = "cancelled"
	p.Comp.EndedAt = p.now()
	call := p.call
	cancel := p.runCancel
	p.mu.Unlock()

	p.publishProjection()
	if call != nil {
		call.Cancel()
	}
	if cancel != nil {
		cancel()
	}
	p.closeDone.Do(func() { close(p.done) })
	return nil
}

// Recv returns the next streaming chunk. On terminal state it
// drains any remaining buffered chunks then returns io.EOF.
func (p *planNodeActor) Recv() (any, error) {
	select {
	case item := <-p.recvCh:
		return item.value, item.err
	case <-p.done:
		// drain remaining buffered items
		select {
		case item := <-p.recvCh:
			return item.value, item.err
		default:
		}
		p.mu.Lock()
		err := p.Comp.ErrorMsg
		p.mu.Unlock()
		if err != "" {
			return nil, errors.New(err)
		}
		return nil, io.EOF
	}
}

// Result blocks until terminal then returns the unary value or
// the terminal error. Returns ErrStreamingResultUnavailable when
// the underlying callable is streaming (no single Result).
func (p *planNodeActor) Result() (any, error) {
	<-p.done
	p.mu.Lock()
	state := p.Comp.State
	errMsg := p.Comp.ErrorMsg
	p.mu.Unlock()
	if state == plan.StateFailed || state == plan.StateCancelled {
		return nil, errors.New(errMsg)
	}
	// Completed — the last Recv value is the Result for unary.
	// For streaming there is no single Result.
	select {
	case item := <-p.recvCh:
		if item.err == io.EOF {
			return item.value, nil
		}
		return item.value, item.err
	default:
	}
	return nil, plan.ErrStreamingResultUnavailable
}

// --- internal goroutines ---

func (p *planNodeActor) runStream(ctx context.Context) {
	call := p.cfg.target.Invoke(ctx, p.cfg.callID, p.cfg.payload)

	p.mu.Lock()
	p.call = call
	p.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			call.Cancel()
			return
		default:
		}

		chunk, err := call.Recv()
		if err == io.EOF {
			p.transitionToCompleted(chunk)
			return
		}
		if err != nil {
			p.transitionToFailed(err)
			return
		}

		select {
		case p.recvCh <- recvResult{value: chunk}:
		case <-ctx.Done():
			call.Cancel()
			return
		case <-p.done:
			call.Cancel()
			return
		}
	}
}

func (p *planNodeActor) runWithTimeout(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		p.runStream(ctx)
		close(done)
	}()

	select {
	case <-done:
		// natural completion
	case <-ctx.Done():
		p.mu.Lock()
		if p.Comp.State.IsTerminal() {
			p.mu.Unlock()
			return
		}
		p.Comp.State = plan.StateCancelled
		p.Comp.ErrorMsg = plan.DiagTimeout
		p.Comp.EndedAt = p.now()
		call := p.call
		p.mu.Unlock()
		if call != nil {
			call.Cancel()
		}
		p.publishProjection()
		p.closeDone.Do(func() { close(p.done) })
	}
}

func (p *planNodeActor) transitionToCompleted(result any) {
	p.mu.Lock()
	if p.Comp.State.IsTerminal() {
		p.mu.Unlock()
		return
	}
	p.Comp.State = plan.StateCompleted
	p.Comp.EndedAt = p.now()
	p.mu.Unlock()

	p.publishProjection()
	select {
	case p.recvCh <- recvResult{value: result, err: io.EOF}:
	default:
	}
	p.closeDone.Do(func() { close(p.done) })
}

func (p *planNodeActor) transitionToFailed(err error) {
	p.mu.Lock()
	if p.Comp.State.IsTerminal() {
		p.mu.Unlock()
		return
	}
	p.Comp.State = plan.StateFailed
	p.Comp.ErrorMsg = err.Error()
	p.Comp.EndedAt = p.now()
	p.mu.Unlock()

	p.publishProjection()
	select {
	case p.recvCh <- recvResult{err: err}:
	default:
	}
	p.closeDone.Do(func() { close(p.done) })
}

func (p *planNodeActor) publishProjection() {
	if p.cfg.projections == nil {
		return
	}
	p.mu.Lock()
	if p.selfRef == nil {
		p.mu.Unlock()
		return
	}
	snap, err := projection.SnapshotOf(p)
	if err != nil {
		p.mu.Unlock()
		return
	}
	selfID := p.selfRef.ID()
	p.mu.Unlock()
	p.cfg.projections.Publish(selfID, projection.Snapshot{
		ActorID: selfID,
		Version: atomic.AddUint64(&p.projVer, 1),
		Fields:  materializeProjectionFields(snap),
	})
}

func (p *planNodeActor) now() time.Time {
	return time.Now()
}

// NewPlanNodeForTest creates a planNodeActor directly for unit tests.
// It bypasses the actor lifecycle (no Cell, no Spawn) so the caller
// must manually drive Start / Stop.
func NewPlanNodeForTest(target ref.Ref, callID string, payload any, opts ...plan.Option) plan.Node {
	cfg := plan.NewConfig(opts...)
	nodeCfg := planNodeConfig{
		target:        target,
		callID:        callID,
		payload:       payload,
		autoStart:     cfg.AutoStart,
		timeout:       cfg.Timeout,
		keepAfterDone: cfg.KeepAfterDone,
		name:          cfg.Name,
	}
	return newPlanNodeActor(nodeCfg)
}

// NewPlanNodeWithRefForTest creates a planNodeActor with an explicit
// selfRef and projections store for projection tests.
func NewPlanNodeWithRefForTest(target ref.Ref, callID string, payload any, selfRef ref.Ref, projections *projection.StoreImpl, opts ...plan.Option) plan.Node {
	cfg := plan.NewConfig(opts...)
	nodeCfg := planNodeConfig{
		target:        target,
		callID:        callID,
		payload:       payload,
		autoStart:     cfg.AutoStart,
		timeout:       cfg.Timeout,
		keepAfterDone: cfg.KeepAfterDone,
		name:          cfg.Name,
		projections:   projections,
	}
	p := newPlanNodeActor(nodeCfg)
	p.setSelfRef(selfRef)
	return p
}
