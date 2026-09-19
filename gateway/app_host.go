package gateway

import (
	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/ref"
	"github.com/qomos-w/gospore/schema"
	"github.com/qomos-w/gospore/tree"
)

// AppHost is the minimal surface of app.App that gateway code needs.
// It exists to break the import cycle between app and gateway.
type AppHost interface {
	Namespace() string
	Self() ref.Ref
	LookupService(name string) (ref.Ref, bool)
	Tree() tree.Tree
	PolicyStore() actor.PolicyStore
	Schemas() schema.Set
	ManifestJSON() []byte
	ExportGosporeManifest() (schema.GosporeManifest, error)

	// SpawnGatewaySession / InvokeAsCaller / DestroyGatewaySession give
	// each gateway connection a dedicated caller cell so reply traffic
	// (and slow-consumer backpressure) never piles onto the root reply
	// pipeline shared by every session. See app.App.
	SpawnGatewaySession(name string) (ref.Ref, error)
	InvokeAsCaller(caller, target ref.Ref, callID string, payload any, headers ...map[string]string) *invoke.Call
	DestroyGatewaySession(r ref.Ref) error
}
