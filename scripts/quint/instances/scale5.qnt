module scale5 {
  import scaling_test(NODE_POOL = Set("n1", "n2", "n3", "n4", "n5"), MAX_REQUESTS = 8,
    FOUNDERS = Map("n1" -> 1, "n2" -> 2, "n3" -> 3), LOAD_IDS = Set("a", "b"),
    INITIAL_LEADER = "n1", MAX_POSITION = 256, MAX_INCARNATION = 2,
    MAX_DELIVERIES = 1024).* from "../tests/scenarios/scaling_test"
}
