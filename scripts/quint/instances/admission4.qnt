module admission4 {
  import readmission_test(NODE_POOL = Set("n1", "n2", "n3", "n4"), MAX_REQUESTS = 3,
    FOUNDERS = Map("n1" -> 1, "n2" -> 2, "n3" -> 3), LOAD_IDS = Set("a", "b"),
    INITIAL_LEADER = "n1", MAX_POSITION = 128, MAX_INCARNATION = 2,
    MAX_DELIVERIES = 512).* from "../tests/scenarios/readmission_test"
}
