//go:build functional

package operator

import (
	"sync"
	"time"
)

type fakeControllerClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeControllerClock() *fakeControllerClock {
	return &fakeControllerClock{now: time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeControllerClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (*fakeControllerClock) NewTimer(delay time.Duration) controllerTimer {
	return systemControllerClock{}.NewTimer(delay)
}
