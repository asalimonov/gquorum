package metaengine

import (
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/internal/recoveryplacement"
	"github.com/asalimonov/gquorum/topology"
)

// RecoveryPlacementContext reads only applied metadata provenance; ordinary
// state retains the exact zero context.
func (m *Machine) RecoveryPlacementContext() (recoveryplacement.Context, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.scoped.recoveryPlacementContext()
}

func (s *scopedState) recoveryPlacementContext() (recoveryplacement.Context, error) {
	if s.importPlacement != nil {
		return *s.importPlacement, nil
	}
	return recoveryplacement.FromImport(s.recoveryPlacementState(), s.Target)
}

func (s *scopedState) recoveryPlacementState() *model.ImportState {
	if s.Recovery == nil {
		return nil
	}
	owned := model.CloneImport(*s.Recovery)
	return &owned
}

func (s *scopedState) validateRecoveryDocument(doc topology.Document) error {
	ctx, err := s.recoveryPlacementContext()
	if err != nil {
		return err
	}
	return recoveryplacement.ValidateDocument(ctx, s.Target, doc, s.Rules)
}

func (s *scopedState) validateRecoveryPlacement(approved, base, desired topology.Document) error {
	ctx, err := s.recoveryPlacementContext()
	if err != nil {
		return err
	}
	return recoveryplacement.Validate(ctx, approved, base, desired, s.Rules)
}

func (s *scopedState) validateRecoveryTransition(base, desired topology.Document, retired map[string]bool) error {
	ctx, err := s.recoveryPlacementContext()
	if err != nil {
		return err
	}
	return recoveryplacement.ValidateTransition(ctx, s.Target, base, desired, s.Rules, retired)
}

func (s *scopedState) validateRecoveryPlan(input topology.PlanInput, desired topology.Document,
	plan topology.Plan) error {
	ctx, err := s.recoveryPlacementContext()
	if err != nil {
		return err
	}
	return recoveryplacement.ValidatePlan(ctx, input, desired, plan)
}
