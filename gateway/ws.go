package gateway

import (
	"math/rand"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var wsUpgradeInFlight atomic.Int32

const (
	wsUpgradeJitterThreshold = 8
	wsUpgradeJitterMax       = 2 * time.Second

	// wsCloseInvalidToken is the private-use WebSocket close code sent when a
	// client PRESENTED a token that failed URLAuth. The 4000-4999 range is
	// application-defined; 4401 = "auth presented but invalid". Clients treat
	// it as "drop cached credentials and re-bootstrap".
	wsCloseInvalidToken = 4401
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
			// A presented-but-invalid token must fail fast: close with 4401
			// instead of proceeding as an anonymous socket. Clients key on
			// the close code to drop their cached credentials and
			// re-bootstrap; a silent anonymous socket turns their claim into
			// an authorization error loop they cannot detect (sporemind
			// glass 2026-10-11).
			if s.logger != nil {
				s.logger.Warn("gateway: ws URL auth failed, rejecting", "err", err, "remote", r.RemoteAddr)
			}
			_ = conn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(wsCloseInvalidToken, "url-auth"),
				time.Now().Add(time.Second))
			return
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
