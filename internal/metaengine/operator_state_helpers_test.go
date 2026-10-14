package metaengine

import (
	"github.com/asalimonov/gquorum/topology"
	"time"
)

func newOperatorRecord(version uint64, attempt uint32, steps []topology.Step, actor string, origin topology.Origin,
	at time.Time) operatorRecord {
	return newOperatorRecordWithPlan(topology.Plan{Version: version, Steps: steps}, attempt, operatorAudit{actor, origin, at})
}
