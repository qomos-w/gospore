package frontgate

import (
	"context"

	"github.com/qomos-w/gospore/id"
)

// AuthProvider authenticates tokens presented by the frontend.
// The zero value (nil provider) means no authentication — all requests
// are treated as anonymous.
type AuthProvider interface {
	// Authenticate validates token and returns the caller identity.
	// Returning a non-nil error causes frontgate to reject the request
	// with an auth-failure diagnostic.
	Authenticate(ctx context.Context, token string) (id.Identity, error)
}

// NopAuthProvider treats every token as anonymous.
type NopAuthProvider struct{}

func (NopAuthProvider) Authenticate(_ context.Context, _ string) (id.Identity, error) {
	return id.Identity{Role: id.RoleAnonymous}, nil
}

var _ AuthProvider = NopAuthProvider{}
