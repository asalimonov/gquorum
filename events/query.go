package events

// Filter selects events by group and kind; empty fields match all values.
type Filter struct {
	GroupID string
	Types   []Type
}

// Query selects a bounded page of journal events after a sequence number.
type Query struct {
	AfterSeq uint64
	Limit    int
	Filter   Filter
}
