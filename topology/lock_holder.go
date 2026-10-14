package topology

// LockHolder reports an observed session and whether the lock is held.
type LockHolder struct {
	Session Session `json:"session"`
	Held    bool    `json:"held"`
}
