package gateway

import (
	"testing"
	"time"
)

// TestWsConnTuningResolveDefaults pins that every zero tuning field
// falls back to its package default, and that explicit values survive.
func TestWsConnTuningResolveDefaults(t *testing.T) {
	got := wsConnTuning{}.resolve()
	want := wsConnTuning{
		stallForceClose:  wsStallForceClose,
		closeDrainWait:   wsCloseDrainTimeout,
		overflowBudget:   wsOverflowBudget,
		outChCapacity:    wsOutChCapacity,
		pingInterval:     wsPingInterval,
		readIdle:         wsReadIdleTimeout,
		writeTimeout:     wsWriteTimeout,
		pingWriteTimeout: wsPingWriteTimeout,
	}
	if got != want {
		t.Fatalf("resolved defaults = %+v, want %+v", got, want)
	}

	// ReadIdle must exceed PingInterval so a ping-only client is never
	// dropped by the idle deadline.
	if want.readIdle <= want.pingInterval {
		t.Fatalf("default readIdle (%v) must exceed pingInterval (%v)", want.readIdle, want.pingInterval)
	}

	explicit := wsConnTuning{
		stallForceClose:  time.Minute,
		closeDrainWait:   time.Second,
		overflowBudget:   1024,
		outChCapacity:    8,
		pingInterval:     time.Second,
		readIdle:         3 * time.Second,
		writeTimeout:     2 * time.Second,
		pingWriteTimeout: 500 * time.Millisecond,
	}.resolve()
	if explicit != (wsConnTuning{
		stallForceClose:  time.Minute,
		closeDrainWait:   time.Second,
		overflowBudget:   1024,
		outChCapacity:    8,
		pingInterval:     time.Second,
		readIdle:         3 * time.Second,
		writeTimeout:     2 * time.Second,
		pingWriteTimeout: 500 * time.Millisecond,
	}) {
		t.Fatalf("explicit values were overridden: %+v", explicit)
	}
}

// TestWSTimeoutsTuningMapsFields pins the Server-level WSTimeouts →
// per-connection wsConnTuning mapping (zero WSTimeouts resolves to all
// defaults via the shared resolve path).
func TestWSTimeoutsTuningMapsFields(t *testing.T) {
	got := WSTimeouts{
		PingInterval:        15 * time.Second,
		ReadIdle:            40 * time.Second,
		WriteTimeout:        10 * time.Second,
		PingWriteTimeout:    2 * time.Second,
		StallForceClose:     5 * time.Minute,
		CloseDrainTimeout:   2 * time.Second,
		OverflowBudgetBytes: 4096,
		OutChCapacity:       16,
	}.tuning()
	if got.pingInterval != 15*time.Second || got.readIdle != 40*time.Second ||
		got.writeTimeout != 10*time.Second || got.pingWriteTimeout != 2*time.Second ||
		got.stallForceClose != 5*time.Minute || got.closeDrainWait != 2*time.Second ||
		got.overflowBudget != 4096 || got.outChCapacity != 16 {
		t.Fatalf("mapping lost fields: %+v", got)
	}

	zero := WSTimeouts{}
	dflt := zero.tuning().resolve()
	pkgDefaults := wsConnTuning{}.resolve()
	if dflt != pkgDefaults {
		t.Fatalf("zero WSTimeouts must resolve to package defaults: %+v", dflt)
	}
}
