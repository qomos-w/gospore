package transport

import (
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/message"
)

// MemoryConnProxy is a test-double ConnProxy backed by a MemoryRemote.
// It is suitable for single-process tests of cross-App connection
// scenarios and as the reference implementation for Phase 2 ConnProxy
// wiring.
type MemoryConnProxy struct {
	actorID  id.ActorID
	identity id.Identity
	remote   *MemoryRemote
}

// NewMemoryConnProxy creates a ConnProxy that sends Frames through the
// given MemoryRemote. The actorID and identity describe the remote actor
// on the other end of the connection.
func NewMemoryConnProxy(actorID id.ActorID, identity id.Identity, remote *MemoryRemote) *MemoryConnProxy {
	return &MemoryConnProxy{
		actorID:  actorID,
		identity: identity,
		remote:   remote,
	}
}

// ActorID returns the remote actor's ID.
func (c *MemoryConnProxy) ActorID() id.ActorID { return c.actorID }

// Identity returns the remote actor's authenticated identity.
func (c *MemoryConnProxy) Identity() id.Identity { return c.identity }

// Send delivers a Frame to the remote actor.
func (c *MemoryConnProxy) Send(frame message.Frame) error {
	return c.remote.Send(frame)
}

// Close shuts down the connection.
func (c *MemoryConnProxy) Close() error {
	return c.remote.Close()
}

var _ ConnProxy = (*MemoryConnProxy)(nil)
