package model

import (
	"time"
)

// ReplayConfig holds the resolved replay parameters.
type ReplayConfig struct {
	Limit    int
	Kinds    []SignalKind
	SinceSeq uint64
}

// SignalFilter provides fine-grained filtering for signal replay queries.
type SignalFilter struct {
	Kinds      []SignalKind `json:"kinds,omitempty"`
	SinceSeq   uint64       `json:"since_seq,omitempty"`
	UntilSeq   uint64       `json:"until_seq,omitempty"`
	Principals []Principal  `json:"principals,omitempty"`
	MinTime    *time.Time   `json:"min_time,omitempty"`
	MaxTime    *time.Time   `json:"max_time,omitempty"`
}

// RetentionPolicy determines how signal payloads are retained.
type RetentionPolicy int

const (
	RetainForever RetentionPolicy = iota
	RetainByDuration
	RetainByCount
)

func (p RetentionPolicy) String() string {
	switch p {
	case RetainForever:
		return "retain_forever"
	case RetainByDuration:
		return "retain_by_duration"
	case RetainByCount:
		return "retain_by_count"
	default:
		return "unknown"
	}
}

// RetentionConfig configures a retention policy with parameters.
type RetentionConfig struct {
	Policy   RetentionPolicy `json:"policy"`
	Duration time.Duration   `json:"duration,omitempty"`
	Count    int             `json:"count,omitempty"`
}

// ReplayOption configures a replay operation.
type ReplayOption func(*ReplayConfig)

// WithReplayLimit sets the maximum number of envelopes to return.
func WithReplayLimit(n int) ReplayOption {
	return func(c *ReplayConfig) { c.Limit = n }
}

// WithReplayKinds filters envelopes to only the specified signal kinds.
func WithReplayKinds(kinds ...SignalKind) ReplayOption {
	return func(c *ReplayConfig) { c.Kinds = kinds }
}

// WithReplaySinceSeq returns envelopes starting from the given sequence number.
func WithReplaySinceSeq(seq uint64) ReplayOption {
	return func(c *ReplayConfig) { c.SinceSeq = seq }
}
