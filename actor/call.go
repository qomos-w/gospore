package actor

import (
	"fmt"

	"github.com/qomos-w/gospore/plan"
	"github.com/qomos-w/gospore/ref"
)

// Call performs a supervised unary invoke. It creates a Plan Node, starts it,
// waits for the result, and returns the decoded typed value. Internally
// equivalent to Plan + Start + Result in a single expression.
//
// The generic parameter T determines the return type. When T is any,
// the raw decoded value is returned without type assertion.
//
// Call requires the actor to have been spawned with Planner capability
// (Props.WithPlanner); otherwise it returns an error.
func Call[T any](ctx Context, target ref.Ref, callID string, payload any, opts ...plan.Option) (T, error) {
	var zero T
	p := ctx.Planner()
	if p == nil {
		return zero, fmt.Errorf("call %s: planner not available", callID)
	}
	node, err := p.Plan(target, callID, payload, opts...)
	if err != nil {
		return zero, fmt.Errorf("call %s: %w", callID, err)
	}
	if err := node.Start(ctx.Lifecycle()); err != nil {
		return zero, fmt.Errorf("call %s: %w", callID, err)
	}
	result, err := node.Result()
	if err != nil {
		return zero, fmt.Errorf("call %s: %w", callID, err)
	}
	if result == nil {
		return zero, nil
	}
	typed, ok := result.(T)
	if !ok {
		return zero, fmt.Errorf("call %s: unexpected result type %T, want %T", callID, result, zero)
	}
	return typed, nil
}

// Stream performs a supervised streaming invoke. It creates a Plan Node, starts
// it, and returns the node ready for Recv iteration. Internally equivalent to
// Plan + Start; the caller reads decoded chunks via node.Recv() until io.EOF.
//
// Stream requires the actor to have been spawned with Planner capability
// (Props.WithPlanner); otherwise it returns an error.
func Stream(ctx Context, target ref.Ref, callID string, payload any, opts ...plan.Option) (plan.Node, error) {
	p := ctx.Planner()
	if p == nil {
		return nil, fmt.Errorf("stream %s: planner not available", callID)
	}
	node, err := p.Plan(target, callID, payload, opts...)
	if err != nil {
		return nil, fmt.Errorf("stream %s: %w", callID, err)
	}
	if err := node.Start(ctx.Lifecycle()); err != nil {
		return nil, fmt.Errorf("stream %s: %w", callID, err)
	}
	return node, nil
}
