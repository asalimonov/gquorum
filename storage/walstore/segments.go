package walstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type segment struct {
	name  string
	seq   uint64
	index uint64
}

func segmentNames(dir string) ([]segment, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []segment
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".wal") {
			continue
		}
		var seq, index uint64
		if _, err := fmt.Sscanf(e.Name(), "%016x-%016x.wal", &seq, &index); err != nil {
			continue
		}
		out = append(out, segment{name: e.Name(), seq: seq, index: index})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].seq < out[j].seq })
	return out, nil
}

func unlinkSegmentsBelow(dir string, limit uint64) error {
	names, err := segmentNames(dir)
	if err != nil {
		return err
	}
	keep := -1
	for i, n := range names {
		if n.index >= limit {
			break
		}
		keep = i
	}
	for i := range keep {
		if err := os.Remove(filepath.Join(dir, names[i].name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("walstore: unlink segment %s: %w", names[i].name, err)
		}
	}
	return nil
}
