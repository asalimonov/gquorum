module foundation {
  import cluster_test(NODE_POOL = Set("n1"), MAX_REQUESTS = 2,
    FOUNDERS = Map(), LOAD_IDS = Set(), INITIAL_LEADER = "n1",
    MAX_POSITION = 3, MAX_INCARNATION = 2, MAX_DELIVERIES = 8).* from "../tests/scenarios/cluster_test"
}
