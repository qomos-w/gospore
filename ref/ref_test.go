package ref

import (
	"context"
	"testing"

	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/invoke"
)

// Ref is an interface-only package. This test verifies the interface
// shape is stable — it will fail at compile time if exported methods
// change signature.
func TestRef_InterfaceShape(t *testing.T) {
	var _ Ref = (*refStub)(nil)
}

type refStub struct{}

func (r *refStub) ID() id.ActorID                         { return id.ActorID{} }
func (r *refStub) Service() (string, bool)                { return "", false }
func (r *refStub) Invoke(ctx context.Context, callID string, payload any, headers ...map[string]string) *invoke.Call {
	return nil
}
