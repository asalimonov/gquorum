package metrics

// HistoryQuery requests a bounded page after a cursor in one observation
// series.
type HistoryQuery struct {
	SeriesID string `json:"series_id"`
	After    uint64 `json:"after"`
	Limit    int    `json:"limit"`
}

// HistoryPage returns observation points and the next cursor for their series.
type HistoryPage struct {
	SeriesID  string               `json:"series_id"`
	Points    []ReplicaObservation `json:"points"`
	Next      uint64               `json:"next"`
	Truncated bool                 `json:"truncated"`
}
