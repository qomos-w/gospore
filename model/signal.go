package model

import "time"

// SignalKind identifies the kind of a signal envelope.
type SignalKind int

const (
	SignalStateChange SignalKind = iota
	SignalMetrics
	SignalError
	SignalLifecycle
	SignalCommand
	SignalPlacement
	SignalMessage
	SignalSnapshot
	SignalCustom
)

func (k SignalKind) String() string {
	switch k {
	case SignalStateChange:
		return "state_change"
	case SignalMetrics:
		return "metrics"
	case SignalError:
		return "error"
	case SignalLifecycle:
		return "lifecycle"
	case SignalCommand:
		return "command"
	case SignalPlacement:
		return "placement"
	case SignalMessage:
		return "message"
	case SignalSnapshot:
		return "snapshot"
	case SignalCustom:
		return "custom"
	default:
		return "unknown"
	}
}

// CrossRef represents a causal reference from one signal to another.
type CrossRef struct {
	Actor ActorRef `json:"actor"`
	Seq   uint64   `json:"seq"`
}

// SignalMeta carries metadata for a signal envelope.
type SignalMeta struct {
	Principal Principal  `json:"principal,omitempty"`
	Runtime   RuntimeID  `json:"runtime,omitempty"`
	CausalIDs []CrossRef `json:"causal_ids,omitempty"`
}

// SignalEnvelope is the storage and transmission unit for actor timeline events.
type SignalEnvelope struct {
	Seq        uint64     `json:"seq"`
	Kind       SignalKind `json:"kind"`
	Actor      ActorRef   `json:"actor"`
	Origin     CrossRef   `json:"origin,omitempty"`
	Meta       SignalMeta `json:"meta,omitempty"`
	Payload    []byte     `json:"payload,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	Persistent bool       `json:"persistent"`
}

// SubscribeOption configures a signal subscription.
type SubscribeOption struct {
	Principal Principal         `json:"principal,omitempty"`
	Scope     ActorObserveScope `json:"scope,omitempty"`
	Kinds     []SignalKind      `json:"kinds,omitempty"`
	SinceSeq  uint64            `json:"since_seq,omitempty"`
}
