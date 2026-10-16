package metaengine

import (
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

const defaultProgressPageLimit = 100

type progressPageHeader struct {
	cluster   string
	query     topology.ProgressQuery
	applied   uint64
	latest    uint64
	target    uint64
	converged topology.FullyConverged
}

// Progress captures owned metadata rows under one read lock. It makes no quorum
// claim; the service uses Query after its quorum/apply barrier.
func (m *Machine) Progress(q topology.ProgressQuery) (topology.ProgressPage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return queryProgress(nil, m, q)
}

func queryProgress(s *scopedState, m *Machine, q topology.ProgressQuery) (topology.ProgressPage, error) {
	if s != nil {
		return queryScopedProgress(s, q)
	}
	return legacyProgress(m, q)
}

func queryScopedProgress(s *scopedState, q topology.ProgressQuery) (topology.ProgressPage, error) {
	if err := q.Validate(); err != nil {
		return topology.ProgressPage{}, err
	}
	if s == nil {
		return topology.ProgressPage{}, fault.ErrUnsupported
	}
	return s.progressLocked(q)
}

// The owning Machine holds its read lock through selection and copying. Only
// selected rows are cloned; operation headers never clone an unbounded block list.
func (s *scopedState) progressLocked(q topology.ProgressQuery) (topology.ProgressPage, error) {
	rows := s.progressRecords(q)
	return pageProgress(progressPageHeader{cluster: s.Latest.ClusterID, query: q, applied: s.AppliedIndex,
		latest: s.Latest.Version, target: s.Target.Version, converged: s.FullyConverged}, rows,
		func(page *topology.ProgressPage, row progressRecord) {
			s.appendProgress(page, row)
		})
}

func pageProgress(header progressPageHeader, rows []progressRecord,
	appendRow func(*topology.ProgressPage, progressRecord),
) (topology.ProgressPage, error) {
	filters := progressFilterHash(header.cluster, header.query)
	last, err := decodeProgressCursor(header.query.After, filters)
	if err != nil {
		return topology.ProgressPage{}, err
	}
	limit := header.query.Limit
	if limit == 0 {
		limit = defaultProgressPageLimit
	}
	slices.SortFunc(rows, func(a, b progressRecord) int { return compareProgressKeys(a.key, b.key) })
	start, _ := slices.BinarySearchFunc(rows, last, func(row progressRecord, key progressKey) int {
		return compareProgressKeys(row.key, key)
	})
	if start < len(rows) && rows[start].key == last {
		start++
	}
	end := start + min(limit, len(rows)-start)
	page := topology.ProgressPage{AppliedIndex: header.applied, ReadAt: time.Now().UTC(),
		LatestVersion: header.latest, TargetVersion: header.target, FullyConverged: header.converged}
	for _, row := range rows[start:end] {
		appendRow(&page, row)
	}
	for i := range page.Operations {
		status := &page.Operations[i]
		status.BlocksTruncated = len(status.Blocks) != status.TotalBlocks
	}
	if end < len(rows) {
		page.Next = encodeProgressCursor(rows[end-1].key, filters)
	}
	return page, nil
}

func (s *scopedState) appendProgress(page *topology.ProgressPage, row progressRecord) {
	switch row.key.Kind {
	case progressGroup:
		page.Groups = append(page.Groups, s.groupProgress(row.key.ID).Clone())
	case progressNode:
		page.Nodes = append(page.Nodes, s.nodeProgress(row.key.ID).Clone())
	case progressOperation:
		s.appendOperationProgress(page, row.operation, row.block)
	case progressCleanup:
		page.Cleanup = append(page.Cleanup, row.cleanup.progress())
	default:
		return // Only the four kinds above are constructed by progressRecords.
	}
}

func (s *scopedState) appendOperationProgress(page *topology.ProgressPage, op *topology.Operation, block int) {
	last := len(page.Operations) - 1
	if last < 0 || page.Operations[last].ID != op.Status.ID {
		header := op.Status
		header.Blocks, header.TotalBlocks = nil, len(op.Status.Blocks)
		page.Operations = append(page.Operations, header)
		last++
	}
	owned := op.Status.Blocks[block]
	if row := s.indexes.groups[owned.GroupID]; row != nil && row.Owner == op.Status.ID {
		owned.Blocked = s.progressBlocker(op.Status.ID, owned.GroupID)
	}
	owned.Blocked = copyCleanupValue(owned.Blocked)
	page.Operations[last].Blocks = append(page.Operations[last].Blocks, owned)
}
