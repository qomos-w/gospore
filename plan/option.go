package plan

import "time"

// Option configures a Plan Node at creation time. Pass to ctx.Plan.
type Option func(*config)

// config is the internal aggregation of Option values.
type config struct {
	name          string
	autoStart     bool
	timeout       time.Duration
	keepAfterDone bool
}

// Config is the exported aggregation of Option values.
// Returned by NewConfig so callers outside the plan package can
// inspect the resolved configuration.
type Config struct {
	Name          string
	AutoStart     bool
	Timeout       time.Duration
	KeepAfterDone bool
}

// NewConfig resolves a slice of Options into a concrete Config.
func NewConfig(opts ...Option) Config {
	c := &config{}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	return Config{
		Name:          c.name,
		AutoStart:     c.autoStart,
		Timeout:       c.timeout,
		KeepAfterDone: c.keepAfterDone,
	}
}

// WithName overrides the auto-generated child name (default
// _plan_<corid>). The name participates in tree path lookup, so it
// must be unique among the parent's existing children.
func WithName(name string) Option {
	return func(c *config) { c.name = name }
}

// WithAutoStart makes the returned Node enter StateRunning immediately
// — Start need not be called. Useful for fire-and-forget workflows
// where the caller only cares about Done / Result.
func WithAutoStart() Option {
	return func(c *config) { c.autoStart = true }
}

// WithTimeout cancels the wrapped Invoke after d elapses since Start.
// On expiry the node transitions to Cancelled; ErrorMsg records
// "timeout".
func WithTimeout(d time.Duration) Option {
	return func(c *config) { c.timeout = d }
}

// WithKeepAfterDone disables the default cleanup that auto-stops the
// node once it reaches a terminal State. Use when the caller intends
// to query Result / Projection after termination.
func WithKeepAfterDone() Option {
	return func(c *config) { c.keepAfterDone = true }
}
