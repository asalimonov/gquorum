package walbackend

import (
	"context"
	"math"

	"github.com/asalimonov/gquorum/internal/recovery/model"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
)

type logReplay struct {
	metadata  []byte
	hardState raftpb.HardState
	entries   []raftpb.Entry
}

// Normal recovery shares strict framing and segment termination. It preserves
// the existing addressable-input ceiling; recovery-specific budgets are supplied
// separately by the offline facade, not imposed on ordinary stores.
func readLog(dir string, point walpb.Snapshot) (logReplay, error) {
	limit := uint64(math.MaxInt - 1)
	evidence, err := readLogStrict(context.Background(), dir, point, model.ReadBounds{MaxRecordBytes: limit,
		MaxGroupBytes: limit})
	if err != nil {
		return logReplay{}, err
	}
	return logReplay{metadata: evidence.Metadata, hardState: evidence.Recovery.HardState,
		entries: evidence.Recovery.Entries}, nil
}
