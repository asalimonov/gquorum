package gquorum

import (
	"time"
)

// ObservationOptions bounds the interval and timeout of peer observations.
type ObservationOptions struct{ Interval, Timeout time.Duration }
