package topology

import "fmt"

// StorageClass selects where a load group's replicas keep their Raft state.
// It is fixed when the group is created.
type StorageClass string

const (
	// StorageDefault keeps Raft state in the Node's persistent load store.
	StorageDefault StorageClass = ""
	// StorageMemory keeps Raft state in the Node-owned in-memory store. It is
	// lost when the process exits; acknowledged writes survive only while a
	// quorum of replicas keeps running.
	StorageMemory StorageClass = "memory"
)

// ValidateStorageClass accepts only StorageDefault and StorageMemory.
func ValidateStorageClass(c StorageClass) error {
	switch c {
	case StorageDefault, StorageMemory:
		return nil
	}
	return invalid(fmt.Sprintf("unknown storage class %q", string(c)))
}
