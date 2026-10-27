module authority3 {
  import effects_test(NODE_POOL = Set("n1", "n2", "n3"), MAX_REQUESTS = 3,
    FOUNDERS = Map("n1" -> 1, "n2" -> 2, "n3" -> 3), LOAD_IDS = Set("a", "b"),
    INITIAL_LEADER = "n1", MAX_POSITION = 16, MAX_INCARNATION = 2,
    MAX_DELIVERIES = 80).* from "../effects_test"
}
