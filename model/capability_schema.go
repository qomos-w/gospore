package model

type CapabilitySchema struct {
	ID        string             `json:"id"`
	Name      string             `json:"name"`
	Kind      string             `json:"kind"`
	Version   string             `json:"version"`
	Callables []CallableProtocol `json:"callables"`
	Metadata  map[string]string  `json:"metadata,omitempty"`
}
