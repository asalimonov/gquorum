package lifecycletest

// HooksCases groups the independent contract cases for lifecycle hooks.
type HooksCases struct {
	Readiness []ReadinessCase
	Retention []RetentionCase
	Removal   []RemovalCase
}
