package contracts

import "github.com/qomos-w/gospore/model"

// Protocol serializes and deserializes actor messages for remote transport.
type Protocol interface {
	Encode(from model.ActorRef, to model.ActorRef, msg interface{}) ([]byte, error)
	Decode(data []byte) (model.ActorRef, model.ActorRef, interface{}, error)
}

// Transport provides network communication between runtime hosts.
type Transport interface {
	Send(runtime model.RuntimeID, data []byte) error
	OnReceive(cb func(from model.RuntimeID, data []byte))
	Start() error
	Stop()
}
