package model

type ExecutionScopeMode int

const (
	ScopeInherit ExecutionScopeMode = iota
	ScopeNone
	ScopeRooted
)

type ActorFactory = func() interface{}

type ActorSpec struct {
	Type                 string
	ID                   ActorID
	Factory              ActorFactory
	Producer             ActorFactory
	Strategy             Directive
	Mailbox              MailboxType
	OwnerPrincipal       Principal
	ExecutionScope       ExecutionScopeSpec
	SnapshotStorageClass SnapshotStorageClass
	Protocols            []CallableProtocol
	Schema               *CapabilitySchema
}

type ExecutionScopeSpec struct {
	Mode ExecutionScopeMode
	Name string
}
