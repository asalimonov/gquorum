# Three-node `_meta` example

```sh
make run-example
make stop-example
```

HTTP endpoints are `http://localhost:18081`, `:18082` and `:18083`. The manifest
supplies two `worker` nodes and one `observer`; all three vote in `_meta`.
The example passes identities, peer addresses, credentials, limits and each
node's `/data/meta.bolt` path to the library. Compose owns the three durable
volumes named `meta-node1`, `meta-node2` and `meta-node3`. These isolate the
small demo from volumes created by an earlier application. Stopping preserves
them; `make clean-example` explicitly deletes them.

This demo contains only startup/shutdown and a small HTTP adapter. Load-group
FSMs, protocols, lifecycle/recovery adapters, WAL fault controls and cluster
scenarios live in `tests/cluster/testapp` and `tests/cluster`.

```sh
api=http://localhost:18081
curl -fsS "$api/api/cluster/status" | jq
curl -fsS "$api/api/topology/active" | jq

version=$(curl -fsS "$api/api/topology/latest" | jq -r .version)
result=$(curl -fsS -X POST "$api/api/nodes/node3/drain" \
  -H "If-Match: $version" -H 'Content-Type: application/json' \
  -d '{"actor":"operator","apply":true}')
execution=$(printf '%s' "$result" | jq -r .execution_id)
curl -fsS "$api/api/executions/$execution" | jq
```

Poll the execution until `state` is `succeeded`. The approved node status becomes
`drained`, and its readiness endpoint returns 503. All three remain meta voters.
Use the current Latest version and `POST /api/nodes/node3/activate` with the same
body to reactivate it. Concurrent/stale version edits return 409. `apply:false`
stages an edit; approve it with `POST /api/topology/{version}/apply`.

Routes: `GET /health/{live,ready}`, `GET /api/cluster/status`,
`GET /api/topology/{latest,target,active}`, `POST /api/nodes/{name}/{drain,activate}`,
`POST /api/topology/{version}/apply`, and `GET /api/executions/{id}`.

`make test-example` runs the actual Compose demo from `tests/cluster/example_test.go`,
using a unique project/image, random host ports and private temporary volumes.
It checks bootstrap, node types, drain/activate, unchanged meta membership,
readiness and stale-version refusal. `make test-fixture` runs the richer
in-process library integration tests under the cluster test tree. Docker Raft
correctness/fault scenarios use `make test-cluster`.
