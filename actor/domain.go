package actor

// DomainHandle is returned by RegisterDomain. It carries the domain
// name and exposes methods to optionally expose the domain as a
// service. Even without calling Expose/ExposeChildren, the domain name
// is registered on the cell for namespace tracking and manifest export.
//
// All methods are nil-safe so that stub implementations can return nil
// from RegisterDomain without callers needing to guard against nil
// before chaining .Expose().
type DomainHandle struct {
	name               string
	exposeFn           func(string) error
	exposeToChildrenFn func(string) error
}

// NewDomainHandle creates a DomainHandle with the given name and optional
// expose/exposeToChildren callbacks. Used by the cell layer to implement
// RegisterDomain; user code receives a DomainHandle from ctx.RegisterDomain.
func NewDomainHandle(name string, exposeFn, exposeToChildrenFn func(string) error) *DomainHandle {
	return &DomainHandle{
		name:               name,
		exposeFn:           exposeFn,
		exposeToChildrenFn: exposeToChildrenFn,
	}
}

// Name returns the domain name.
func (d *DomainHandle) Name() string {
	if d == nil {
		return ""
	}
	return d.name
}

// Expose registers this domain as an App-level service, making it
// discoverable via LookupService by other actors. Equivalent to the
// former ctx.Expose(name).
func (d *DomainHandle) Expose() error {
	if d == nil || d.exposeFn == nil {
		return nil
	}
	return d.exposeFn(d.name)
}

// ExposeChildren registers this domain as a scoped service visible only
// to actors in this cell's descendant subtree. Equivalent to the former
// ctx.ExposeToChildren(name).
func (d *DomainHandle) ExposeChildren() error {
	if d == nil || d.exposeToChildrenFn == nil {
		return nil
	}
	return d.exposeToChildrenFn(d.name)
}