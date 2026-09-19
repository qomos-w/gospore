package gateway

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// stallConn is a FrameConn whose Send always fails, to verify session.send
// closes the connection so the whole session tears down instead of wedging.
type stallConn struct {
	mu       sync.Mutex
	closed   bool
	sendErr  error
	recvBlocked chan struct{}
}

func newStallConn() *stallConn {
	return &stallConn{recvBlocked: make(chan struct{})}
}

func (s *stallConn) Recv() (*WireFrame, error) {
	<-s.recvBlocked // block until close
	return nil, errors.New("closed")
}

func (s *stallConn) Send(*WireFrame) error { return errors.New("write deadline exceeded") }

func (s *stallConn) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.recvBlocked)
	}
	return nil
}

func (s *stallConn) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// TestSession_SendErrorClosesConn verifies that a failed write (e.g. write
// deadline exceeded on a stalled peer) triggers conn.Close, unblocking the
// read loop so ServeSession returns instead of leaking the session.
func TestSession_SendErrorClosesConn(t *testing.T) {
	conn := newStallConn()
	srv := &Server{} // logger nil is handled

	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.ServeSession(context.Background(), conn, SessionOptions{})
	}()

	// Wait for the session to be running, then force a send failure.
	// There is no exported send; drive it via the auth-timeout path: an
	// anonymous session with URLAuth configured would send an error frame
	// on timeout — instead poke send directly through a short-lived
	// session value replicating ServeSession's construction.
	sess := &session{server: srv, conn: conn, role: "anonymous", subject: ""}
	sess.send(wsFrame{Type: "chunk", SubID: "sub-1"})

	deadline := time.After(3 * time.Second)
	for {
		if conn.isClosed() {
			break
		}
		select {
		case <-deadline:
			t.Fatal("conn not closed within 3s after send failure")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Close unblocks Recv, so the ServeSession goroutine finishes.
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("ServeSession did not return after conn close")
	}
}
