package topology

// Update is a coalesced document-watch notification, not a complete event log.
type Update struct {
	View     View
	Document Document
}
