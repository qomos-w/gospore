package cell

import (
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/internal/handler"
	"github.com/qomos-w/gospore/mailbox"
	"github.com/qomos-w/gospore/message"
	"github.com/qomos-w/gospore/supervisor"
)

// Cell state constants for the lifecycle state machine.
// Call dispatch, loop resolution, panic recovery, and policy checks.
// ---------------------------------------------------------------------------
// Dispatch & Invoke
// ---------------------------------------------------------------------------

func (c *Cell) dispatchCall(env mailbox.Envelope, reply func(message.Frame)) {
	if denied, diag := c.checkPolicy(env); denied {
		c.logger.Error("cell: dispatchCall POLICY_DENIED", "callID", env.Frame.CallID, "diag", diag)
		reply(message.Frame{Kind: message.KindError, Body: []byte(diag)})
		reply(message.Frame{Kind: message.KindEnd})
		return
	}

	switch env.Frame.CallID {
	case actor.CallIDReload:
		c.handleControlCallReply(env, reply, func(ie *handler.InvokeEnv) error {
			req, ok, err := decodeReloadReq(ie.Payload)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			return newCallContext(c, *ie).applyScriptReload(req)
		})
		return
	case actor.CallIDReplace:
		c.handleControlCallReply(env, reply, func(ie *handler.InvokeEnv) error {
			req, ok, err := decodeReplaceReq(ie.Payload)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			return newCallContext(c, *ie).applyScriptReplace(req)
		})
		return
	}

	if h := c.Handlers(); h == nil {
		c.logger.Error("cell: dispatchCall NO_HANDLERS", "callID", env.Frame.CallID)
		reply(message.Frame{Kind: message.KindError, Body: []byte("gospore.cell: handler table not initialized")})
		reply(message.Frame{Kind: message.KindEnd})
		return
	}
	inv, ok := c.Handlers().Lookup(env.Frame.CallID)
	if !ok {
		c.logger.Error("cell: dispatchCall NOT_FOUND", "callID", env.Frame.CallID)
		reply(message.Frame{Kind: message.KindError, Body: []byte(fmt.Sprintf("gospore.cell: call ID %q not registered", env.Frame.CallID))})
		reply(message.Frame{Kind: message.KindEnd})
		return
	}

	loop := c.resolveCallLoop(env, inv)
	switch loop {
	case actor.DefaultLoopPure:
		// Should not reach here — stateless calls are handled in handleCall
		// and run in a goroutine. This is a safety fallback.
		c.timedInvoke(laneNamePure, inv, env, reply)
		return
	case actor.DefaultLoopOwner:
		c.timedInvoke(laneNameOwner, inv, env, reply)
		return
	default:
		if !c.enqueueLoop(loop, env) {
			// Lane full after bounded patience (or draining). Reply an
			// explicit error; executing inline on the caller's goroutine
			// (owner loop or pure fallback) would run the handler
			// concurrently with the busy custom lane.
			reply(message.Frame{Kind: message.KindError, Body: []byte(fmt.Sprintf(
				"cell: custom lane %q unavailable (full or draining), call dropped explicitly", loop))})
			reply(message.Frame{Kind: message.KindEnd})
		}
		return
	}
}

func (c *Cell) resolveCallLoop(env mailbox.Envelope, inv *handler.Invoker) string {
	if inv == nil {
		return actor.DefaultLoopOwner
	}
	if loop, ok := c.selfLoopOverride(env); ok {
		if !c.isRegisteredLoop(loop) {
			return actor.DefaultLoopOwner
		}
		if loop == actor.DefaultLoopPure && inv.Mode != actor.ModeStateless {
			return actor.DefaultLoopOwner
		}
		return loop
	}
	switch inv.Loop {
	case actor.DefaultLoopPure:
		if inv.Mode == actor.ModeStateless {
			return actor.DefaultLoopPure
		}
		return actor.DefaultLoopOwner
	case actor.DefaultLoopOwner:
		return actor.DefaultLoopOwner
	case "", actor.DefaultLoopReply:
		return actor.DefaultLoopForMode(inv.Mode)
	default:
		if c.isRegisteredLoop(inv.Loop) {
			c.loopsMu.Lock()
			lane, exists := c.loops[inv.Loop]
			c.loopsMu.Unlock()
			if exists && lane != nil && lane.mode == actor.ModeStateless && inv.Mode != actor.ModeStateless {
				return actor.DefaultLoopOwner
			}
			return inv.Loop
		}
		return actor.DefaultLoopOwner
	}
}

func (c *Cell) isRegisteredLoop(name string) bool {
	switch name {
	case actor.DefaultLoopOwner:
		return true
	case actor.DefaultLoopPure:
		return true
	case actor.DefaultLoopReply, "":
		return false
	}
	c.loopsMu.Lock()
	_, ok := c.loops[name]
	c.loopsMu.Unlock()
	return ok
}

func (c *Cell) selfLoopOverride(env mailbox.Envelope) (string, bool) {
	if env.Frame.Headers == nil {
		return "", false
	}
	loop := strings.TrimSpace(env.Frame.Headers["gospore.loop"])
	if loop == "" {
		return "", false
	}
	if env.Sender == nil || c.self == nil || env.Sender.ID() != c.self.ID() {
		return "", false
	}
	if role := strings.TrimSpace(env.Frame.Headers["gospore.caller_role"]); role != "" {
		return "", false
	}
	switch loop {
	case actor.DefaultLoopOwner, actor.DefaultLoopPure:
		return loop, true
	default:
		if c.isRegisteredLoop(loop) {
			return loop, true
		}
		return "", false
	}
}

func (c *Cell) invokeLoop(name string, env mailbox.Envelope) {
	reply := c.makeReply(env)
	if h := c.Handlers(); h == nil {
		reply(message.Frame{Kind: message.KindError, Body: []byte("gospore.cell: handler table not initialized")})
		reply(message.Frame{Kind: message.KindEnd})
		return
	}
	inv, ok := c.Handlers().Lookup(env.Frame.CallID)
	if !ok {
		reply(message.Frame{Kind: message.KindError, Body: []byte(fmt.Sprintf("gospore.cell: call ID %q not registered", env.Frame.CallID))})
		reply(message.Frame{Kind: message.KindEnd})
		return
	}
	c.invokeLoopCall(name, inv, env, reply)
}

func (c *Cell) invokeLoopCall(name string, inv *handler.Invoker, env mailbox.Envelope, reply func(message.Frame)) {
	c.pureWG.Add(1)
	defer c.pureWG.Done()
	c.timedInvoke(name, inv, env, reply)
}

func (c *Cell) invokeCall(inv *handler.Invoker, env mailbox.Envelope, reply func(message.Frame)) {
	var invokeErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				if c.logger != nil {
					c.logger.Error("gospore/cell: handler panic",
						"actorID", c.self.ID().String(),
						"callID", env.Frame.CallID,
						"panic", r,
						"stack", string(debug.Stack()),
					)
				}
				invokeErr = fmt.Errorf("actor panic: %v", r)
				c.recoverAndDecide(r)
				reply(message.Frame{Kind: message.KindError, Body: []byte(invokeErr.Error())})
				reply(message.Frame{Kind: message.KindEnd})
			}
		}()
		c.emitCallableEvent(actor.WatchInvokeInStarted, inv, env, "")
		var payload any
		if env.Frame.PayloadMode == message.PayloadModeValue {
			payload = env.Frame.Body
			if env.Payload != nil {
				payload = env.Payload
			}
		}
		invokeEnv := handler.InvokeEnv{
			Frame:          env.Frame,
			Caller:         env.Sender,
			Payload:        payload,
			Actor:          c.currentActor(),
			Reply:          reply,
			Codec:          c.codec,
			ReturnSchemaID: inv.ReturnSchemaID,
			ChunkSchemaID:  inv.ChunkSchemaID,
			Identity:       identityFromHeaders(env.Frame.Headers),
			OnCancel: func(corID uint64, cancel func()) {
				c.replyReg.registerCancel(corID, cancel)
			},
			OffCancel: func(corID uint64) {
				c.replyReg.unregisterCancel(corID)
			},
		}
		invokeEnv.Context = newCallContext(c, invokeEnv)
		if inv.Script != nil {
			if sr := c.scriptRuntimeRef(); sr != nil {
				if err := sr.invoke(inv, invokeEnv); err != nil {
					invokeErr = err
					invokeEnv.Reply(message.Frame{Kind: message.KindError, Body: []byte(err.Error())})
					invokeEnv.Reply(message.Frame{Kind: message.KindEnd})
					return
				}
			} else {
				inv.Run(invokeEnv)
			}
		} else {
			inv.Run(invokeEnv)
		}
		_, _, _ = c.applyProjectionIfObserved()
		c.emitCallableEvent(actor.WatchInvokeInEnded, inv, env, invokeErrString(invokeErr))
	}()
	if c.monitor != nil {
		c.monitor.OnCall(c.cellID(), inv.CallID, invokeErr)
	}
}

func (c *Cell) handleControlCallReply(env mailbox.Envelope, reply func(message.Frame), apply func(*handler.InvokeEnv) error) {
	ie := handler.InvokeEnv{
		Frame:    env.Frame,
		Caller:   env.Sender,
		Payload:  env.Frame.Body,
		Actor:    c.currentActor(),
		Reply:    reply,
		Codec:    c.codec,
		Identity: identityFromHeaders(env.Frame.Headers),
	}
	if err := apply(&ie); err != nil {
		reply(message.Frame{Kind: message.KindError, Body: []byte(err.Error())})
		reply(message.Frame{Kind: message.KindEnd})
		return
	}
	reply(message.Frame{Kind: message.KindEnd})
}

func decodeReloadReq(payload any) (actor.ReloadReq, bool, error) {
	switch req := payload.(type) {
	case actor.ReloadReq:
		return req, true, nil
	case *actor.ReloadReq:
		if req == nil {
			return actor.ReloadReq{}, true, nil
		}
		return *req, true, nil
	case []byte:
		if len(req) == 0 {
			return actor.ReloadReq{}, false, nil
		}
		var out actor.ReloadReq
		if err := json.Unmarshal(req, &out); err != nil {
			return actor.ReloadReq{}, false, fmt.Errorf("decode %s: %w", actor.CallIDReload, err)
		}
		return out, true, nil
	default:
		return actor.ReloadReq{}, false, nil
	}
}

func decodeReplaceReq(payload any) (actor.ReplaceReq, bool, error) {
	switch req := payload.(type) {
	case actor.ReplaceReq:
		return req, true, nil
	case *actor.ReplaceReq:
		if req == nil {
			return actor.ReplaceReq{}, true, nil
		}
		return *req, true, nil
	case []byte:
		if len(req) == 0 {
			return actor.ReplaceReq{}, false, nil
		}
		var out actor.ReplaceReq
		if err := json.Unmarshal(req, &out); err != nil {
			return actor.ReplaceReq{}, false, fmt.Errorf("decode %s: %w", actor.CallIDReplace, err)
		}
		return out, true, nil
	default:
		return actor.ReplaceReq{}, false, nil
	}
}

func (c *Cell) cellID() string {
	if c.self == nil {
		return ""
	}
	return c.self.ID().String()
}

func (c *Cell) routeToWaitingStream(env mailbox.Envelope) {
	c.replyReg.deliver(env.Frame)
}

func (c *Cell) cancelStream(env mailbox.Envelope) {
	c.replyReg.cancel(env.Frame.CorID)
}

func (c *Cell) recoverAndDecide(recovered any) {
	if c.logger != nil {
		c.logger.Error("gospore/cell: actor panic recovered",
			"actorID", c.self.ID().String(),
			"panic", recovered,
			"stack", string(debug.Stack()),
		)
	}
	reason := fmt.Errorf("actor panic: %v", recovered)
	decision := c.supervisor.Decide(c.self, reason)
	switch decision {
	case supervisor.Restart:
		c.Recv(mailbox.Envelope{Payload: mailbox.Restart{Reason: reason}})
	case supervisor.Stop:
		c.Recv(mailbox.Envelope{Payload: mailbox.Stop{}})
	case supervisor.Escalate:
		c.Recv(mailbox.Envelope{Payload: mailbox.Stop{}})
		c.notifyParentEscalation(reason)
	case supervisor.Resume:
	}
}

func (c *Cell) notifyParentEscalation(reason error) {
	if c.parent != nil && c.deliveryHost != nil {
		_ = c.deliveryHost.Deliver(c.parent, mailbox.Envelope{Payload: mailbox.Escalated{Child: c.self, Reason: reason}})
	} else if c.deliveryHost != nil {
		c.deliveryHost.RootEscalated(reason)
	}
}

func (c *Cell) handleEscalated(m mailbox.Escalated) {
	decision := c.supervisor.Decide(m.Child, m.Reason)
	switch decision {
	case supervisor.Restart:
		c.Recv(mailbox.Envelope{Payload: mailbox.Restart{Reason: m.Reason}})
	case supervisor.Stop:
		c.Recv(mailbox.Envelope{Payload: mailbox.Stop{}})
	case supervisor.Escalate:
		c.notifyParentEscalation(m.Reason)
	case supervisor.Resume:
	}
}

func identityFromHeaders(headers map[string]string) id.Identity {
	if len(headers) == 0 {
		return id.Identity{}
	}
	role := id.Role(strings.TrimSpace(headers["gospore.caller_role"]))
	if role == "" {
		return id.Identity{}
	}
	ident := id.Identity{Role: role}
	if kindStr := strings.TrimSpace(headers["gospore.caller_kind"]); kindStr != "" {
		switch kindStr {
		case id.IdentityToken.String():
			ident.Kind = id.IdentityToken
		case id.IdentityAppCertificate.String():
			ident.Kind = id.IdentityAppCertificate
		default:
			ident.Kind = id.IdentityAnonymous
		}
		ident.Subject = strings.TrimSpace(headers["gospore.caller_subject"])
		return ident
	}
	ident.Kind = id.IdentityToken
	ident.Subject = strings.TrimSpace(headers["gospore.caller_subject"])
	return ident
}

func (c *Cell) checkPolicy(env mailbox.Envelope) (bool, string) {
	if c.policyStore == nil {
		return false, ""
	}

	role := ""
	if env.Frame.Headers != nil {
		role = strings.TrimSpace(env.Frame.Headers["gospore.caller_role"])
	}
	isExternal := role != ""
	callID := env.Frame.CallID

	if isExternal && strings.HasPrefix(callID, "_gospore_.") {
		return true, actor.DiagPolicyDenied
	}
	if !isExternal {
		return false, ""
	}

	allow, found := c.policyStore.Evaluate(id.Role(role), callID)
	if found && !allow {
		return true, actor.DiagPolicyDenied
	}
	return false, ""
}

func isCriticalSystemMsg(payload any) bool {
	switch payload.(type) {
	case mailbox.Start, mailbox.Stop, mailbox.Restart, mailbox.Escalated:
		return true
	default:
		return false
	}
}
