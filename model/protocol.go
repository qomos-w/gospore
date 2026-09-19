package model

type InvocationMode string

type StreamEventKind string

type ProtocolTypeKind string

const (
	ModeSend    InvocationMode = "send"
	ModeRequest InvocationMode = "request"
	ModeInvoke  InvocationMode = "invoke"
)

const (
	StreamEventNext  StreamEventKind = "next"
	StreamEventFinal StreamEventKind = "final"
	StreamEventError StreamEventKind = "error"
)

const (
	ProtocolTypeVoid   ProtocolTypeKind = "void"
	ProtocolTypeScalar ProtocolTypeKind = "scalar"
	ProtocolTypeArray  ProtocolTypeKind = "array"
	ProtocolTypeMap    ProtocolTypeKind = "map"
	ProtocolTypeStruct ProtocolTypeKind = "struct"
)

type ProtocolType struct {
	Kind       ProtocolTypeKind `json:"kind"`
	Name       string           `json:"name,omitempty"`
	Element    *ProtocolType    `json:"element,omitempty"`
	Key        *ProtocolType    `json:"key,omitempty"`
	Value      *ProtocolType    `json:"value,omitempty"`
	StructName string           `json:"struct_name,omitempty"`
}

type ProtocolParameter struct {
	Name string       `json:"name"`
	Type ProtocolType `json:"type"`
}

type StreamingProtocol struct {
	NextType  *ProtocolType             `json:"next_type,omitempty"`
	FinalType *ProtocolType             `json:"final_type,omitempty"`
	Message   *MessageStreamingProtocol `json:"message,omitempty"`
}

type MessageStreamingProtocol struct {
	Start *ProtocolType `json:"start,omitempty"`
	Delta *ProtocolType `json:"delta,omitempty"`
	End   *ProtocolType `json:"end,omitempty"`
}

type CallableProtocol struct {
	Name       string               `json:"name"`
	Mode       InvocationMode       `json:"mode"`
	Parameters []ProtocolParameter  `json:"parameters,omitempty"`
	Returns    []ProtocolType       `json:"returns,omitempty"`
	HasError   bool                 `json:"has_error,omitempty"`
	Streaming  *StreamingProtocol   `json:"streaming,omitempty"`
}
