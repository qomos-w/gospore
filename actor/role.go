package actor

import (
	"github.com/qomos-w/gospore/id"
)

// Role is an alias to id.Role for convenience in actor package users.
type Role = id.Role

// Predefined role aliases for convenience.
const (
	RoleAnonymous = id.RoleAnonymous
	RoleAgent     = id.RoleAgent
	RoleAdmin     = id.RoleAdmin
)

// ValidRoleName is an alias to id.ValidRoleName.
var ValidRoleName = id.ValidRoleName
