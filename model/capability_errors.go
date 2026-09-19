package model

import "errors"

var (
	ErrCapabilityRouteRequired   = errors.New("gospore: capability route required")
	ErrCapabilityCallableNotFound = errors.New("gospore: capability callable not found")
)
