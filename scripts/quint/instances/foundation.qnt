module foundation {
  import cluster_test(NODE_POOL = Set("n1"), MAX_REQUESTS = 2).* from "../cluster_test"
}
