package topology

// IdentityLookup records a validated durable identity and whether its pin exists.
// An unreadable pin may be present while the accompanying error rejects its contents.
type IdentityLookup struct {
	Identity Identity
	Found    bool
}
