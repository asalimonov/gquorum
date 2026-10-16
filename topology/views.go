package topology

// View selects an owned, locally applied document observation.
type View string

// Latest is staged intent; Target is approved intent. Progress is observed
// separately, without a synthetic converged document.
const (
	Latest View = "latest"
	Target View = "target"
)

// Page selects history entries in descending version order. Zero Limit is
// unbounded.
type Page struct{ Offset, Limit int }

// HistoryResult returns owned topology documents and the oldest retained
// version.
type HistoryResult struct {
	Documents     []Document
	OldestVersion uint64
}
