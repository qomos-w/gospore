package actor

// Hot-reload protocol wire format per ARCHITECTURE.md §4.20.
//
// `_gospore_.script_reload` and `_gospore_.script_replace` are the two
// CallIDs every actor's host installs at OnStart, regardless of whether
// the actor currently has any script content. They live under the
// reserved `_gospore_` namespace (the only namespace allowed to start
// with an underscore) so they can never collide with user-defined
// CallIDs in any user-chosen Namespace (`^[a-z][a-z0-9_]*$`).
//
// The two protocols differ in policy, not in surface:
//
//   - script_reload performs the §4.20 compatibility-
//     constrained extension: add new components / handlers, change
//     handler source, change Meta. Removing or renaming preserves
//     state by failing the request rather than mutating data.
//   - script_replace performs the destructive variant: any change is
//     accepted, but state is dropped. Caller must explicitly opt in via
//     ReplaceReq.DropState — guarding against accidental data loss is
//     the whole point of the explicit flag.
const (
	CallIDReload  = "_gospore_.script_reload"
	CallIDReplace = "_gospore_.script_replace"
)

// ReloadReq is the §4.20 request payload for
// `_gospore_.script_reload`. Definition is the new (compatibility-
// constrained) Definition to install; the Cell-tier classifier
// inspects oldDef vs Definition against the §4.20 table
// before swapping.
//
// A nil Definition is illegal — reload must always carry the new
// shape; "remove all script content" is conceptually a destructive
// operation and must use ReplaceReq{Definition: nil, DropState: true}
// instead.
type ReloadReq struct {
	Definition *Definition
}

// ReplaceReq is the §4.20 request payload for
// `_gospore_.script_replace`. Definition is the new Definition to
// install (may be nil to wipe the data layer); DropState MUST be set
// to true to confirm the caller accepts the loss of dynamic component
// state. ValidateReplaceReq enforces this invariant at the data layer
// before the Cell consumes the request.
type ReplaceReq struct {
	Definition *Definition
	DropState  bool
}
