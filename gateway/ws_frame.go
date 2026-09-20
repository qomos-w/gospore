package gateway

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gorilla/websocket"

	"github.com/qomos-w/gospore/schema"
	"github.com/qomos-w/spore/identity"
	"github.com/qomos-w/spore/transport"
)

type wsFrame struct {
	Type       string          `json:"type"`
	ReqID      int64           `json:"reqId,omitempty"`
	CallID     string          `json:"callID,omitempty"`
	Target     string          `json:"target,omitempty"`
	From       string          `json:"from,omitempty"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	SubID      string          `json:"subId,omitempty"`
	SinceSeqNo int64           `json:"sinceSeqNo,omitempty"`
	SeqNo      int64           `json:"seqNo,omitempty"`
	TransID    int64           `json:"transId,omitempty"`
	Message    string          `json:"message,omitempty"`
	TimeoutMs  int64           `json:"timeoutMs,omitempty"`
	// payloadIsBinary is set when the incoming wire frame carried a
	// binary-encoded (TBC) payload. It prevents the gateway from
	// JSON-unmarshalling the payload before passing it to the actor.
	payloadIsBinary bool
}

// wsUpgrader allows all origins (matching the HTTP CORS policy).
var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(_ *http.Request) bool { return true },
}

// ---------------------------------------------------------------------------
// Frame type mapping
// ---------------------------------------------------------------------------

var wsTypeToFrameType = map[string]FrameType{
	"invoke":      FrameTypeInvoke,
	"reply":       FrameTypeReply,
	"error":       FrameTypeError,
	"chunk":       FrameTypeChunk,
	"end":         FrameTypeEnd,
	"subscribe":   FrameTypeSubscribe,
	"unsubscribe": FrameTypeUnsubscribe,
	"auth":        FrameTypeAuth,
	"auth_ok":     FrameTypeAuthOk,
	"ping":        FrameTypePing,
	"pong":        FrameTypePong,
}

var frameTypeToWsType = map[FrameType]string{
	FrameTypeInvoke:      "invoke",
	FrameTypeReply:       "reply",
	FrameTypeError:       "error",
	FrameTypeChunk:       "chunk",
	FrameTypeEnd:         "end",
	FrameTypeSubscribe:   "subscribe",
	FrameTypeUnsubscribe: "unsubscribe",
	FrameTypeAuth:        "auth",
	FrameTypeAuthOk:      "auth_ok",
	FrameTypePing:        "ping",
	FrameTypePong:        "pong",
}

func (s *Server) wireFrameToWsFrame(wire *WireFrame) (*wsFrame, error) {
	payload, err := Decompress(wire.Payload, wire.Flags.Compression())
	if err != nil {
		return nil, err
	}
	if s.binaryOnly && wire.Flags.Encoding() == EncodingJSON {
		return nil, errors.New("binary-only: JSON-encoded payload not allowed")
	}
	sinceSeqNo := int64(0)
	seqNo := int64(wire.Seq)
	if frameTypeToWsType[wire.Type] == "subscribe" {
		sinceSeqNo = int64(wire.Seq)
		seqNo = 0
	}
	return &wsFrame{
		Type:            frameTypeToWsType[wire.Type],
		ReqID:           int64(wire.CorID),
		CallID:          wire.CallID,
		Target:          wire.Target,
		From:            wire.From,
		Payload:         payload,
		SubID:           wire.SubID,
		SinceSeqNo:      sinceSeqNo,
		SeqNo:           seqNo,
		TransID:         int64(wire.TransID),
		Message:         wire.ErrorMsg,
		payloadIsBinary: wire.Flags.Encoding() == EncodingBinary,
	}, nil
}

// wsTBCPreamble mirrors codec.binaryMagicPreamble: spore >= v0.6.0 splits
// the TBC header into a 3-byte magic plus an independent version byte;
// the sniff routes on the preamble only (version semantics belong to the
// codec backend).
var wsTBCPreamble = []byte{0x54, 0x42, 0x43} // "TBC"

func isTBCData(data []byte) bool {
	return len(data) >= 4 && string(data[:3]) == string(wsTBCPreamble)
}

func wsFrameToWireFrame(f *wsFrame) (*WireFrame, error) {
	ft, ok := wsTypeToFrameType[f.Type]
	if !ok {
		ft = FrameTypeError
	}
	payload := []byte(f.Payload)
	compressed, comp, err := Compress(payload)
	if err != nil {
		return nil, err
	}
	enc := EncodingJSON
	if isTBCData(payload) {
		enc = EncodingBinary
	}
	// For subscribe frames, carry SinceSeqNo in the Seq slot so the
	// binary protocol can resume subscriptions. For all other types,
	// Seq carries the chunk SeqNo.
	seq := f.SeqNo
	if f.Type == "subscribe" {
		seq = f.SinceSeqNo
	}
	return &WireFrame{
		Flags:    MakeFlags(enc, comp),
		Type:     ft,
		CorID:    uint64(f.ReqID),
		Seq:      uint32(seq),
		TransID:  uint64(f.TransID),
		CallID:   f.CallID,
		SubID:    f.SubID,
		Target:   f.Target,
		From:     f.From,
		ErrorMsg: f.Message,
		Payload:  compressed,
	}, nil
}

// ---------------------------------------------------------------------------
// Auth helpers
// ---------------------------------------------------------------------------

// encodeAuthOk builds a TBC-encoded AuthOk{Manifest: manifestJSON} payload.
func encodeAuthOk(manifestJSON []byte) json.RawMessage {
	desc, _ := schema.BuiltinDesc(schema.BuiltinAuthOk)
	_, _ = schema.BuiltinObject(schema.BuiltinAuthOk)
	val := map[string]any{"Manifest": string(manifestJSON)}
	id, _ := identity.NewCanonicalID(0, 0, 0, 0)
	bc := &transport.BinaryCodec{}
	view, err := bc.Encode(desc, id, val)
	if err != nil {
		return nil
	}
	return json.RawMessage(view.Data)
}

// decodeAuthReq decodes a TBC-encoded AuthReq payload and returns the token.
func decodeAuthReq(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	if !isTBCData(payload) {
		return string(payload)
	}
	desc, _ := schema.BuiltinDesc(schema.BuiltinAuthReq)
	bc := &transport.BinaryCodec{}
	view := transport.View{Kind: transport.ViewKindFull, Schema: desc, Data: payload}
	result, err := bc.Decode(view)
	if err != nil {
		return string(payload)
	}
	if m, ok := result.(map[string]any); ok {
		if t, ok := m["Token"].(string); ok {
			return t
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// WebSocket adapter — implements FrameConn over *websocket.Conn
// ---------------------------------------------------------------------------

// ErrBinaryOnlyTextFrame is returned by wsFrameConn.Recv when a text frame
// arrives on a binary-only connection. The session sends an error frame to
// the peer before closing.
var ErrBinaryOnlyTextFrame = errors.New("binary-only: text frame not allowed")

// ---------------------------------------------------------------------------
// Outbound never-blocking contract (aligned with the desktop Wails raw
// transport): Send only marshals and enqueues; a dedicated writeLoop owns the
// socket; a slow or suspended peer can stall only its own connection — never
// the gateway session, whose worker pool serializes on sendMu around Send,
// and never other clients.
