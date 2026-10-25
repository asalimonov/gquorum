package recoverymodel

// TornTail identifies the final undecoded region; it never raises retained commit.
type TornTail struct {
	File   string
	Offset int64
}
