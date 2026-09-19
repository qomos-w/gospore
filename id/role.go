package id

import "strings"

// Role is the identity role of a caller. Supports custom values.
// Inheritance is expressed via dot notation:
//
//	"agent.admin" inherits "agent"
//	"partner.vip" inherits "partner"
//
// The dot convention is enforced by Effective() without any external
// configuration.
type Role string

const (
	// RoleAnonymous is the unauthenticated caller.
	RoleAnonymous Role = "anonymous"
	// RoleAgent is a standard agent caller.
	RoleAgent Role = "agent"
	// RoleAdmin is an administrator caller.
	RoleAdmin Role = "admin"
)

// Effective returns the inheritance chain of a role from most specific
// to least specific. For a non-dotted role the result is a single-element
// slice. For "agent.admin" the result is ["agent.admin", "agent"].
func (r Role) Effective() []Role {
	s := string(r)
	if !strings.Contains(s, ".") {
		return []Role{r}
	}
	parts := strings.Split(s, ".")
	result := make([]Role, len(parts))
	for i := 0; i < len(parts); i++ {
		result[len(parts)-1-i] = Role(strings.Join(parts[:i+1], "."))
	}
	return result
}

// ValidRoleName reports whether s is a valid role identifier:
// [a-z][a-z0-9_]*(.[a-z][a-z0-9_]*)*
func ValidRoleName(s string) bool {
	if s == "" {
		return false
	}
	segStart := true
	for i := 0; i < len(s); i++ {
		c := s[i]
		if segStart {
			if !(c >= 'a' && c <= 'z') {
				return false
			}
			segStart = false
			continue
		}
		switch {
		case c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9':
		case c == '_':
		case c == '.':
			segStart = true
		default:
			return false
		}
	}
	return !segStart
}
