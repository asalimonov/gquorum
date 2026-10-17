package topology

// AuthorityGrantLookup pairs an exact validated grant with its presence.
type AuthorityGrantLookup struct {
	Grant AuthorityGrant
	Found bool
}

// EffectReceiptLookup pairs validated completion evidence with its presence.
type EffectReceiptLookup struct {
	Receipt EffectReceipt
	Found   bool
}
