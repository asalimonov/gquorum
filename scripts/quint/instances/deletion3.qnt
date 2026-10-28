module deletion3 {
  import groups_test(NODE_POOL = Set("n1", "n2", "n3"), MAX_REQUESTS = 6,
    FOUNDERS = Map("n1" -> 1, "n2" -> 2, "n3" -> 3), LOAD_IDS = Set("a", "b"),
    INITIAL_LEADER = "n1", MAX_POSITION = 64, MAX_INCARNATION = 2,
    MAX_DELIVERIES = 256).* from "../tests/scenarios/groups_test"
}
