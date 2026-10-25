package walbackend

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
)

// Called with Store.mu held, before any writer can acquire the published seed.
func (s *Store) acquireRecoveryGroup(groupID string) error {
	key, protected := s.protection[groupID]
	if !protected {
		return nil
	}
	found, err := s.findSeedControl(context.Background(), key)
	if err != nil {
		return err
	}
	if found == nil {
		return fault.ErrOperationAuthorization
	}
	root, err := s.rootLease.OpenRecoveryRoot(found.name)
	if err != nil {
		return err
	}
	defer root.Close()
	if _, err := s.resumeSeed(context.Background(), root, *found); err != nil {
		return err
	}
	if found.control.State == "consumed" {
		return nil
	}
	if err := s.seedBoundary("acquired"); err != nil {
		return err
	}
	found.control.State = "acquired"
	return writeSeedControl(root, found.name, found.control, true)
}
