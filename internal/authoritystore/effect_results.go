package authoritystore

import "github.com/asalimonov/gquorum/topology"

// ReceiptLookup preserves exact effect evidence and its presence.
type ReceiptLookup = topology.EffectReceiptLookup

// EffectLookup preserves a completed effect and its presence.
type EffectLookup struct {
	Record EffectRecord
	Found  bool
}
