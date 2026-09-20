// Package mailbox holds the Envelope type and system-message payloads
// (Watch/Unwatch/Stop and similar control-plane messages that travel in
// the Payload slot). Despite the historical name there is no queue here:
// delivery is Cell.Recv-driven via DeliveryHost.Deliver.
package mailbox

import (
	"time"

	"github.com/qomos-w/gospore/message"
	"github.com/qomos-w/gospore/ref"
)

// Envelope is the unit carried between actors. It pairs a Frame with
// optional pre-decoded Payload (kept around for in-process delivery so
// we can skip a round of encode/decode).
type Envelope struct {
	// Sender is the originating actor's reference, or nil for transport-
	// generated frames.
	Sender ref.Ref
	// Frame is the on-wire frame.
	Frame message.Frame
	// Payload is the locally-known Go value (set when the sender and
	// recipient share the address space and the value is reusable
	// without re-decoding). May be nil; consumers that require a
	// concrete value must fall back to decoding Frame.Body.
	Payload any
	// EnqueuedAt records when the cell runtime placed this envelope onto
	// an ingress lane queue; zero when the envelope was never queued
	// (stateless calls dispatch straight to a goroutine). Set only by
	// the cell runtime to measure queue-wait per lane. Envelope is an
	// in-process type and this field is never serialised onto the wire.
	EnqueuedAt time.Time
}
