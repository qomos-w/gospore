package app

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/ref"
	"github.com/qomos-w/spore/identity"
)

// sysMsgWire is the JSON envelope for cross-process system messages.
type sysMsgWire struct {
	Kind    string   `json:"k"`
	Watcher string   `json:"w,omitempty"`
	Of      string   `json:"o,omitempty"`
	Kinds   []string `json:"kinds,omitempty"`
}

// sysRef is a minimal ref.Ref sufficient for system-message routing.
// It carries only an ActorID; Invoke and Service are stubbed.
type sysRef struct {
	aid id.ActorID
}

func (r sysRef) ID() id.ActorID                          { return r.aid }
func (r sysRef) Service() (string, bool)                 { return "", false }
func (r sysRef) Invoke(context.Context, string, any, ...map[string]string) *invoke.Call { return nil }

var _ ref.Ref = sysRef{}

// encodeSystemMsg serializes Watch / Unwatch / Terminated into JSON bytes.
// Returns an error for unsupported system-message types.
func encodeSystemMsg(msg mailbox.SystemMsg) ([]byte, error) {
	var wire sysMsgWire
	switch m := msg.(type) {
	case mailbox.Watch:
		wire.Kind = "watch"
		wire.Watcher = m.Watcher.ID().String()
		for _, k := range m.Kinds {
			if s := k.String(); s != "" {
				wire.Kinds = append(wire.Kinds, s)
			}
		}
	case mailbox.Unwatch:
		wire.Kind = "unwatch"
		wire.Watcher = m.Watcher.ID().String()
	case mailbox.Terminated:
		wire.Kind = "terminated"
		wire.Of = m.Of.ID().String()
	default:
		return nil, fmt.Errorf("app: unsupported system message %T", msg)
	}
	return json.Marshal(wire)
}

// decodeSystemMsg deserializes JSON bytes into Watch / Unwatch / Terminated.
func decodeSystemMsg(body []byte) (mailbox.SystemMsg, error) {
	var wire sysMsgWire
	if err := json.Unmarshal(body, &wire); err != nil {
		return nil, err
	}
	switch wire.Kind {
	case "watch":
		cid, err := identity.ParseCanonicalID(wire.Watcher)
		if err != nil {
			return nil, fmt.Errorf("app: parse watcher id: %w", err)
		}
		var kinds []actor.WatchKind
		for _, s := range wire.Kinds {
			k, ok := actor.ParseWatchKind(s)
			if !ok {
				return nil, fmt.Errorf("app: unknown watch kind %q", s)
			}
			kinds = append(kinds, k)
		}
		return mailbox.Watch{
			Watcher: sysRef{aid: id.From(cid)},
			Kinds:   kinds,
		}, nil
	case "unwatch":
		cid, err := identity.ParseCanonicalID(wire.Watcher)
		if err != nil {
			return nil, fmt.Errorf("app: parse watcher id: %w", err)
		}
		return mailbox.Unwatch{
			Watcher: sysRef{aid: id.From(cid)},
		}, nil
	case "terminated":
		cid, err := identity.ParseCanonicalID(wire.Of)
		if err != nil {
			return nil, fmt.Errorf("app: parse terminated id: %w", err)
		}
		return mailbox.Terminated{
			Of: sysRef{aid: id.From(cid)},
		}, nil
	default:
		return nil, fmt.Errorf("app: unknown system message kind %q", wire.Kind)
	}
}
