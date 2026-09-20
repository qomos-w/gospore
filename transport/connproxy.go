package transport

import (
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/message"
)

// ConnProxy represents a long-lived external connection in the actor
// tree without a cell goroutine. It has an ActorID but no mailbox, no
// handler registration, and no supervisor participation.
//
// Transport servers create one ConnProxy per accepted connection.
// The proxy's ActorID is used as Frame.From for calls originating
// from this connection. Watch and Event/State subscription updates
// are pushed directly to the connection without traversing a mailbox.
//
// See ARCHITECTURE.md §4.17 for the full design.
type ConnProxy interface {
	// ActorID returns the proxy's unique identifier.
	ActorID() id.ActorID

	// Identity returns the Gate-1 handshake identity.
	Identity() id.Identity

	// Send delivers a frame to the external caller.
	Send(frame message.Frame) error

	// Close shuts the connection and triggers tree cleanup.
	Close() error
}
