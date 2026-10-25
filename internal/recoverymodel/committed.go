package recoverymodel

import (
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
)

// CommittedPrefix selects the source's retained commit without changing any
// evidence. The result shares immutable source bytes; callers must not mutate it.
// Full source classification/fingerprinting precedes this interpreter/seed view.
func CommittedPrefix(source storage.Recovery) (storage.Recovery, error) {
	commit := source.HardState.Commit
	if commit < source.Snapshot.Metadata.Index {
		return storage.Recovery{}, fault.ErrUnreadable
	}
	last := source.Snapshot.Metadata.Index
	count := 0
	for _, entry := range source.Entries {
		if entry.Index > commit {
			break
		}
		last = entry.Index
		count++
	}
	if last != commit {
		return storage.Recovery{}, fault.ErrUnreadable
	}
	prefix := source
	prefix.Entries = source.Entries[:count:count]
	return prefix, nil
}
