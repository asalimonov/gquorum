package snapshot

import "context"

// Service is a borrowed view of a Node-owned transfer coordinator, not a separate
// lifecycle or application snapshot export/import API. Transports authenticate
// the declared sender before invoking ingress methods; the owner separately
// checks exact peer authorization and run identity, including after waits.
type Service interface {
	// Options returns a copy of the resolved construction-time policy.
	Options() Options
	Begin(context.Context, BeginRequest) (Ack, error)
	Write(context.Context, Chunk) (Ack, error)
	Finish(context.Context, Session) (Ack, error)
	// Detach fences this connection while retaining a resumable transfer.
	Detach(context.Context, Session) error
	// Abort cancels the exact transfer; a timed-out waiter does not free work
	// still owned by its workers or the native engine.
	Abort(context.Context, Session) error
	// SessionDone closes on attachment fencing, expiry or owner stop. Unknown
	// and stale sessions return an already closed signal.
	SessionDone(Session) <-chan struct{}
	// List returns bounded local observations, never quorum or install proof.
	List(Query) Page
}
