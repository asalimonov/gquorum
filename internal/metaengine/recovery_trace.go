package metaengine

// The optional observer records only commands actually committed while import
// was pending. Snapshot the predicate before Apply and notify after persistence,
// outside the FSM lock. Ordinary machines pay no decoding/closure cost.
func (m *Machine) recoveryCommandTrace(data []byte) func() {
	observer := m.OnRecoveryCommand
	if observer == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.scoped == nil || m.scoped.Recovery == nil || m.scoped.Recovery.Phase != "importing" {
		return nil
	}
	envelope, err := decodeEnvelope(data)
	if err != nil {
		return nil
	}
	allowed := m.scoped.recoveryCommandAllowed(envelope.Kind, envelope.Body)
	return func() { observer(envelope.Kind, allowed) }
}
