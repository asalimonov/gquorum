module scoped3 {
  import scheduler_test(NODE_POOL = Set("n1", "n2", "n3"), MAX_REQUESTS = 4,
    FOUNDERS = Map("n1" -> 1, "n2" -> 2, "n3" -> 3), LOAD_IDS = Set("a", "b"),
    INITIAL_LEADER = "n1", MAX_POSITION = 128, MAX_INCARNATION = 2,
    MAX_DELIVERIES = 512).* from "../tests/scenarios/scheduler_test"
}
