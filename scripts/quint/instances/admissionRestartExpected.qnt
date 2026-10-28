module admissionRestartExpected {
  import types.* from "../types"
  import admission_test(NODE_POOL = Set("n1", "n2", "n3", "n4"), MAX_REQUESTS = 3,
    FOUNDERS = Map("n1" -> 1, "n2" -> 2, "n3" -> 3), LOAD_IDS = Set("a", "b"),
    INITIAL_LEADER = "n1", MAX_POSITION = 128, MAX_INCARNATION = 2,
    MAX_DELIVERIES = 512).* from "../tests/scenarios/admission_test"

  // Desired automatic recovery; intentionally outside positive profiles.
  // Reproduce the source-level admission gap documented in docs/reports.
  run retainedPreparationRestartShouldContinueTest = unacknowledgedPreparedRestart
    .then(runJoinNative(AddLearner))
}
