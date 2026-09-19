package scriptbridge

import "github.com/qomos-w/gospore/internal/handler"

// Host is the SPI scriptbridge consumes from gospore's App tier to
// build the dynamic `gospore.invoke.<callID>` capability set. It is
// the only gospore→scriptbridge contract beyond the public
// app.App interface itself: an App implementation that carries a
// runtime handler table must satisfy Host so scriptbridge can
// enumerate the registered callIDs.
//
// app.appImpl satisfies Host implicitly via its HandlerTable method;
// test doubles in tdd/ also declare conformance. Replacing this with
// an inline duck-typed interface assertion (the historical pattern)
// would hide the contract from readers and tooling — keeping it as a
// named interface makes the boundary explicit and self-documenting.
type Host interface {
	// HandlerTable returns the App's runtime handler.Table. The
	// returned table is read-only from scriptbridge's perspective —
	// scriptbridge enumerates CallIDs and Desc snapshots for the
	// dynamic gospore.invoke capability and never mutates the table.
	HandlerTable() *handler.Table
}
