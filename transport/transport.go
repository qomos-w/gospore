// Package transport is the Frame delivery abstraction.
//
// Phase 1 ships only the Local implementation, which routes Frames
// directly into target Cells' mailboxes within the same process.
// Phase 2 adds cross-process implementations (TCP / WebSocket / QUIC)
// without altering the Cell or Handler layers.
//
// The Transport interface deliberately does NOT do address resolution:
// "which ActorID lives at which network address" is a discovery
// concern (see discovery.Provider) external to this package. Transport
// only ferries bytes between addresses already provided to it.
//
// Lane routing. Transport carries application traffic (`message.Frame`
// with Kind = Call/Reply/Error/End) and is required to enqueue those
// frames on the target mailbox's user lane (`Mailbox.PushUser`). It
// MUST NOT be used to deliver system control messages such as Start /
// Stop / Watch / Terminated / Escalated — those travel as `Envelope.Payload`
// values defined in `mailbox/system_msgs.go` and are written to the
// system lane (`Mailbox.PushSystem`) by the App's `deliver` closure,
// which Transport never sees. A future Transport that wants to expose
// system events across the network will have to re-encode them as
// dedicated frames and translate them back at the receiver before
// reaching PushSystem; the current contract intentionally does not
// admit a `lane` parameter on Send.
package transport

import (
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/message"
)

// RecvRegistry is the local actor recv function index.
//
// Transport implementations that operate in-process (Local) double as
// the registry because the recv function lives in the same address space.
// Cross-process Transport implementations do NOT implement this
// interface; the App maintains a separate RecvRegistry for its
// local actors and routes remote frames through Transport.Send.
type RecvRegistry interface {
	Register(target id.ActorID, recv func(mailbox.Envelope) error)
	Unregister(target id.ActorID)
	Lookup(target id.ActorID) (func(mailbox.Envelope) error, bool)
}

// Transport is the Frame delivery contract.
type Transport interface {
	// Send delivers one Frame to the user lane of the target mailbox.
	//
	// Local impl: looks up Frame.To in the App tree and writes the
	// envelope via Mailbox.PushUser.
	// Remote impl: serialises Frame and ships via the network; the
	// receiver dispatches the decoded frame back through PushUser.
	//
	// Send must not be used for system control messages. See the
	// package-level comment for the reasoning.
	Send(frame message.Frame) error

	// Receive returns the channel of incoming frames.
	// Local impl: returns nil — local frames bypass Receive and are
	// delivered straight to the target Cell.
	// Remote impl: returns network-received frames, which gospore
	// internally routes to the corresponding Cell.
	Receive() <-chan message.Frame

	// Close shuts the transport down and releases its resources
	// (connections, goroutines, etc.). Safe to call multiple times.
	Close() error
}
