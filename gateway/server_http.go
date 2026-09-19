package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// HTTP handlers: call/invoke/subscribe/health/logs/schema + CORS.
func (s *Server) handleCall(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}

	// 1. Extract callID from path: /api/{callID}
	callID := strings.TrimPrefix(r.URL.Path, "/api/")
	if callID == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "missing call ID"})
		return
	}

	// 2. Read and parse body.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "bad body"})
		return
	}
	var payload any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid json"})
			return
		}
	}

	// 3. Extract identity headers.
	customerID := r.Header.Get("X-Customer-ID")
	role := r.Header.Get("X-Role")
	subject := r.Header.Get("X-Subject")
	target := r.URL.Query().Get("target")
	from := r.URL.Query().Get("from")

	req := &GatewayRequest{
		CallID:     callID,
		CustomerID: customerID,
		Role:       role,
		Service:    serviceFromCallID(callID),
		Target:     target,
		From:       from,
		Payload:    payload,
		Headers: map[string]string{
			"Content-Type": r.Header.Get("Content-Type"),
		},
	}

	// 4. Interceptor Before (sync).
	if err := s.interceptor.Before(r.Context(), req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		if errors.Is(err, ErrGatewayDenied) {
			w.WriteHeader(http.StatusForbidden)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	// 5. Resolve target actor.
	targetRef, ok := s.resolveTarget(req.Service, callID, req.Target, req.From)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "service not found"})
		return
	}

	// 6. Invoke actor through a per-request gatesession caller cell.
	start := time.Now()
	hdrs := map[string]string{"gospore.caller_role": role}
	if subject != "" {
		hdrs["gospore.caller_subject"] = subject
	}
	gate, gateDone := s.sessionGate("http")
	defer gateDone()
	call := s.invokeAs(gate, targetRef, r.Context(), callID, payload, hdrs)
	if call == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invoke failed"})
		return
	}
	defer call.Close()

	raw, err := call.RecvRaw()
	duration := time.Since(start)

	resp := &GatewayResponse{
		Body:     raw,
		Duration: duration,
	}
	if err != nil && !errors.Is(err, io.EOF) {
		resp.Error = err
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
	} else {
		w.Header().Set("Content-Type", "application/json")
		if raw == nil {
			w.WriteHeader(http.StatusNoContent)
		} else {
			_, _ = w.Write(raw)
		}
	}

	// 7. Interceptor After (async, fire-and-forget).
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				// Swallow interceptor panics to avoid crashing the gateway.
			}
		}()
		s.interceptor.After(r.Context(), req, resp)
	}()
}

func (s *Server) handleInvoke(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}

	// 1. Parse body.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "bad body"})
		return
	}
	var reqBody struct {
		CallID    string `json:"callID"`
		Target    string `json:"target,omitempty"`
		From      string `json:"from,omitempty"`
		Payload   any    `json:"payload"`
		TimeoutMs int64  `json:"timeoutMs,omitempty"`
	}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &reqBody); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid json"})
			return
		}
	}
	if reqBody.CallID == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "missing callID"})
		return
	}

	// 2. Build gateway request.
	customerID := r.Header.Get("X-Customer-ID")
	role := r.Header.Get("X-Role")
	subject := r.Header.Get("X-Subject")
	service := serviceFromCallID(reqBody.CallID)
	from := r.URL.Query().Get("from")
	if reqBody.From != "" {
		from = reqBody.From
	}
	req := &GatewayRequest{
		CallID:     reqBody.CallID,
		CustomerID: customerID,
		Role:       role,
		Service:    service,
		Target:     reqBody.Target,
		From:       from,
		Payload:    reqBody.Payload,
		Headers: map[string]string{
			"Content-Type": r.Header.Get("Content-Type"),
		},
	}

	// 3. Interceptor Before.
	if err := s.interceptor.Before(r.Context(), req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		if errors.Is(err, ErrGatewayDenied) {
			w.WriteHeader(http.StatusForbidden)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	// 4. Resolve target.
	targetRef, ok := s.resolveTarget(service, reqBody.CallID, reqBody.Target, from)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "service not found"})
		return
	}

	// 5. Invoke.
	start := time.Now()
	hdrs := map[string]string{"gospore.caller_role": role}
	if subject != "" {
		hdrs["gospore.caller_subject"] = subject
	}
	invokeCtx := r.Context()
	timeoutCancel := func() {}
	if reqBody.TimeoutMs > 0 {
		invokeCtx, timeoutCancel = context.WithTimeout(r.Context(), time.Duration(reqBody.TimeoutMs)*time.Millisecond)
	}
	defer timeoutCancel()
	gate, gateDone := s.sessionGate("http")
	defer gateDone()
	call := s.invokeAs(gate, targetRef, invokeCtx, reqBody.CallID, reqBody.Payload, hdrs)
	if call == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invoke failed"})
		return
	}
	defer call.Close()

	raw, err := call.RecvRaw()
	duration := time.Since(start)

	resp := &GatewayResponse{
		Body:     raw,
		Duration: duration,
	}
	if err != nil && !errors.Is(err, io.EOF) {
		resp.Error = err
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
	} else {
		w.Header().Set("Content-Type", "application/json")
		if raw == nil {
			w.WriteHeader(http.StatusNoContent)
		} else {
			_, _ = w.Write(raw)
		}
	}

	// 6. Interceptor After.
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
			}
		}()
		s.interceptor.After(r.Context(), req, resp)
	}()
}

func (s *Server) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}

	// 1. Parse query params.
	callID := r.URL.Query().Get("callID")
	if callID == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "missing callID"})
		return
	}
	var payload any
	if payloadStr := r.URL.Query().Get("payload"); payloadStr != "" {
		if err := json.Unmarshal([]byte(payloadStr), &payload); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid payload json"})
			return
		}
	}

	// 2. Build gateway request.
	customerID := r.Header.Get("X-Customer-ID")
	role := r.Header.Get("X-Role")
	subject := r.Header.Get("X-Subject")
	service := serviceFromCallID(callID)
	target := r.URL.Query().Get("target")
	from := r.URL.Query().Get("from")
	req := &GatewayRequest{
		CallID:     callID,
		CustomerID: customerID,
		Role:       role,
		Service:    service,
		Target:     target,
		From:       from,
		Payload:    payload,
		Headers: map[string]string{
			"Content-Type": r.Header.Get("Content-Type"),
		},
	}

	// 3. Interceptor Before.
	if err := s.interceptor.Before(r.Context(), req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		if errors.Is(err, ErrGatewayDenied) {
			w.WriteHeader(http.StatusForbidden)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	// 4. Resolve target.
	targetRef, ok := s.resolveTarget(service, callID, target, from)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "service not found"})
		return
	}

	// 5. Set up SSE.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, ok := w.(http.Flusher)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "streaming unsupported"})
		return
	}

	// 6. Invoke and stream through a per-connection gatesession caller
	// cell, destroyed when the SSE request ends.
	hdrs := map[string]string{"gospore.caller_role": role}
	if subject != "" {
		hdrs["gospore.caller_subject"] = subject
	}
	gate, gateDone := s.sessionGate("sse")
	defer gateDone()
	call := s.invokeAs(gate, targetRef, r.Context(), callID, payload, hdrs)
	if call == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invoke failed"})
		return
	}
	defer call.Close()

	// Cancel the in-flight call when the HTTP client disconnects
	// so RecvRaw() unblocks and the handler can exit.
	go func() {
		<-r.Context().Done()
		call.Cancel()
	}()

	start := time.Now()
	resp := &GatewayResponse{}

	for {
		raw, err := call.RecvRaw()
		if err == io.EOF {
			resp.Duration = time.Since(start)
			fmt.Fprintf(w, "data: %s\n\n", `{"done":true}`)
			flusher.Flush()
			break
		}
		if err != nil {
			resp.Error = err
			resp.Duration = time.Since(start)
			fmt.Fprintf(w, "data: %s\n\n", fmt.Sprintf(`{"error":"%s"}`, err.Error()))
			flusher.Flush()
			break
		}
		fmt.Fprintf(w, "data: %s\n\n", raw)
		flusher.Flush()
	}

	// 7. Interceptor After.
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
			}
		}()
		s.interceptor.After(r.Context(), req, resp)
	}()
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}
	if s.logProvider == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "log provider not configured"})
		return
	}

	q := r.URL.Query()
	limit := 200
	if n, err := strconv.Atoi(q.Get("limit")); err == nil && n > 0 {
		limit = n
	}
	opts := LogQuery{
		Level:  q.Get("level"),
		Caller: q.Get("caller"),
		Limit:  limit,
	}

	entries := s.logProvider.QueryLogs(opts)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(entries)
}

func (s *Server) handleSchema(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}

	data, err := s.app.Schemas().Marshal()
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(data)
}

// sessionGate spawns a transient gatesession caller cell for one gateway
// connection or request. Invoke sites route through app.InvokeAsCaller with
// the returned ref so reply slots land in the connection's own pending
// table; DestroyGatewaySession (via the returned cleanup) flushes that
// table with terminal errors, unblocking in-flight calls.
//
// A nil ref (spawn failed, e.g. app teardown already in progress) means
// callers fall back to legacy root-caller semantics — availability over
// isolation on the shutdown path.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && origin != "null" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
		} else {
			w.Header().Set("Access-Control-Allow-Origin", "*")
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Customer-ID, X-Role, X-Subject")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// serviceFromCallID extracts the leading segment before the first dot.
// For "quota.check" it returns "quota"; for "ping" it returns "ping".
func serviceFromCallID(callID string) string {
	if i := strings.Index(callID, "."); i > 0 {
		return callID[:i]
	}
	return callID
}
