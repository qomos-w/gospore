package contracts

import "github.com/qomos-w/gospore/model"

// OwnerReader is the canonical read model for querying actors by owner.
type OwnerReader interface {
	ActorsByOwner(filter model.OwnerQueryFilter) []*model.ActorInfo
}
