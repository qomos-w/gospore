package model

import "errors"

var (
	ErrActorIDTimestampOverflow = errors.New("gospore: actor id timestamp exceeds 48 bits")
	ErrActorIDSequenceOverflow  = errors.New("gospore: actor id sequence exceeds 48 bits")
)
