package contracts

import "github.com/qomos-w/gospore/model"

// PayloadCodec encodes and decodes signal payload data.
type PayloadCodec interface {
	Name() string
	Encode(v interface{}) ([]byte, error)
	Decode(data []byte, v interface{}) error
}

// ReplayReader provides read access to signal history for one actor timeline.
type ReplayReader interface {
	Replay(ref model.ActorRef, opts ...model.ReplayOption) ([]model.SignalEnvelope, error)
	ReplayFiltered(ref model.ActorRef, filter model.SignalFilter) ([]model.SignalEnvelope, error)
	ReplayChain(ref model.ActorRef, seq uint64) ([]model.SignalEnvelope, error)
}
