package model

import "time"

// Future represents a pending request-response interaction.
type Future interface {
	Await(timeout time.Duration) (interface{}, error)
	OnComplete(cb func(interface{}, error))
}

// Stream is the response stream returned from an invocation.
type Stream interface {
	Next() (interface{}, error)
	Await(timeout time.Duration) (interface{}, error)
	Cancel()
}
