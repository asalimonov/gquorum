package gquorum

import (
	"context"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
)

func recordManagedRecoverySeed(ctx context.Context, journal *recoverydisk.Journal,
	progress model.SeedProgress) error {
	states := []string{"pending", "prepared", "installed", "replayed", "consumed"}
	target := slices.Index(states, progress.State)
	if target < 1 {
		return fault.ErrInvalid
	}
	for {
		record := journal.View()
		position := slices.IndexFunc(record.Seeds,
			func(seed model.SeedProgress) bool { return seed.GroupID == progress.GroupID })
		if position < 0 {
			return fault.ErrIdentityMismatch
		}
		old := record.Seeds[position]
		rank := slices.Index(states, old.State)
		if old.Attempt != 0 && (old.Destination != progress.Destination ||
			old.MarkerDigest != progress.MarkerDigest ||
			old.Attempt != progress.Attempt) {
			return fault.ErrIdentityMismatch
		}
		if rank >= target {
			return nil
		}
		next := progress
		next.State = states[rank+1]
		if rank+1 < slices.Index(states, "replayed") {
			next.Checkpoint = model.Position{}
		}
		if err := journal.MarkSeed(ctx, next); err != nil {
			return err
		}
	}
}
