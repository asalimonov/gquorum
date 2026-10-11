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

This demo creates a `gquorum.Node` from caller-supplied `NodeOptions`, calls
`Start` once, and uses `node.Cluster()` for topology operations. `Stop` joins
shutdown. The HTTP adapter checks Node readiness and the Active node state.
Load-group FSMs, protocols, application storage, WAL fault controls and cluster
scenarios live in `tests/cluster/testapp` and `tests/cluster`.

```sh
api=http://localhost:18081
curl -fsS "$api/api/cluster/status" | jq
curl -fsS "$api/api/topology/active" | jq

version=$(curl -fsS "$api/api/topology/latest" | jq -r .version)
result=$(curl -fsS -X POST "$api/api/nodes/node3/drain" \
  -H "If-Match: $version" -H 'Content-Type: application/json' \
  -d '{"actor":"operator","approve":true}')
operator=$(printf '%s' "$result" | jq -r .operator_id)
curl -fsS "$api/api/operators/$operator" | jq
```

Poll the operator until `state` is `succeeded`; `failed` and `superseded` are
terminal failures. A follower can briefly return 404 before observing the
operator. HTTP 202 confirms acceptance, so wait for convergence before the next
edit. The approved node status becomes
`drained`, and its readiness endpoint returns 503. All three remain meta voters.
Reactivate it with a separately staged edit and approval:

```sh
version=$(curl -fsS "$api/api/topology/latest" | jq -r .version)
staged=$(curl -fsS -X POST "$api/api/nodes/node3/activate" \
  -H "If-Match: $version" -H 'Content-Type: application/json' \
  -d '{"actor":"operator","approve":false}')
version=$(printf '%s' "$staged" | jq -r .document.version)
operator=$(curl -fsS -X POST "$api/api/topology/$version/approve" | jq -r .operator_id)
curl -fsS "$api/api/operators/$operator" | jq
```

Concurrent/stale version edits return 409. Mutations require one positive
`If-Match` version, bare or quoted. `preview:true` previews an edit without
staging it and cannot combine with `approve:true`. The approval endpoint accepts
an empty body or `{}`, no query parameters, and an optional matching `If-Match`.
If staging commits but approval fails, `X-Staged-Version` identifies the document
to inspect and approve.

Routes: `GET /health/{live,ready}`, `GET /api/cluster/status`,
`GET /api/topology/{latest,target,active}`, `POST /api/nodes/{name}/{drain,activate}`,
`POST /api/topology/{version}/approve`, and `GET /api/operators/{id}`.

`make test-example` runs the actual Compose demo from `tests/cluster/example_test.go`,
using a unique project/image, random host ports and private temporary volumes.
It checks bootstrap, node types, both approval flows, previews, drain/activate,
unchanged meta membership, readiness, stale-version refusal and restart from
retained volumes. `make test-fixture` runs the richer
in-process library integration tests under the cluster test tree. Docker Raft
correctness/fault scenarios use `make test-cluster`.
