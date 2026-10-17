//go:build !functional

package metaengine

type failingPersister struct{ err error }

func (p failingPersister) SaveState([]byte) error { return p.err }
