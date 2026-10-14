package load

import (
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// ActionRequest binds a requested load effect to its grant and exact
// authorization tuple.
type ActionRequest struct {
	Grant         topology.AuthorityGrant
	From          topology.Identity
	To            Peer
	Authorization topology.AuthorizedStep
	// LocalTarget identifies a host effect. For founding, an empty target asks
	// the running group to persist its separate native membership receipt.
	LocalTarget storage.Identity
}

// ActionResult returns effect receipts and the exact authorization they
// acknowledge.
type ActionResult struct {
	Receipts      []topology.EffectReceipt
	From          Peer
	Authorization topology.AuthorizedStep
	LocalTarget   storage.Identity
}
