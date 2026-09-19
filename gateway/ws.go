package gateway

import (
	"math/rand"
	"net/http"
	"sync/atomic"
	"time"
)

var wsUpgradeInFlight atomic.Int32

const (
	wsUpgradeJitterThreshold = 8
	wsUpgradeJitterMax       = 2 * time.Second
)

// to ServeSession for all protocol logic.
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if wsUpgradeInFlight.Add(1) > wsUpgradeJitterThreshold {
		time.Sleep(time.Duration(rand.Int63n(int64(wsUpgradeJitterMax) + 1)))
	}
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	wsUpgradeInFlight.Add(-1)
	if err != nil {
		if s.logger != nil {
			s.logger.Error("gateway: ws upgrade", "err", err)
		}
		return
	}

	wsTuning := s.wsTimeouts.tuning().resolve()
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(wsTuning.readIdle))
		return nil
	})

	customerID := r.Header.Get("X-Customer-ID")
	role := "anonymous"
	subject := ""

	// Optional connection-time auth via URL query.
	var urlAuth URLAuth
	if s.urlAuth != nil {
		if authRole, authSubject, err := s.urlAuth(r.Context(), r.URL.Query(), s.app); err == nil {
			role = authRole
			subject = authSubject
			if s.logger != nil {
				s.logger.Info("gateway: ws URL auth", "role", role, "subject", subject, "remote", r.RemoteAddr)
			}
		} else if r.URL.Query().Get("token") != "" {
			if s.logger != nil {
				s.logger.Warn("gateway: ws URL auth failed", "err", err, "remote", r.RemoteAddr)
			}
		}
		// When urlAuth is configured, enable frame-level auth handshake.
		urlAuth = s.urlAuth
	}

	fc := newWsFrameConnTuned(conn, s.logger, s.binaryOnly, wsTuning)
	defer fc.Close()
	s.ServeSession(r.Context(), fc, SessionOptions{
		CustomerID: customerID,
		Role:       role,
		Subject:    subject,
		URLAuth:    urlAuth,
	})
}
