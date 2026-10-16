# Three-node `_meta` example

This is a separate Go application module using Kong for CLI/environment parsing
and the standard `net/http` server. Its files separate the application concerns:

| File | Responsibility |
| --- | --- |
| `main.go` | Commands, process signals and joined application shutdown. |
| `config.go` | Flags, environment variables and the typed JSON peer manifest. |
| `cluster.go` | Node construction, founding identities, address resolution and readiness rules. |
| `http.go` | HTTP server, routes, request validation, JSON/error responses and health probes. |

Run the demo from the repository root with Docker Compose:

```sh
make run-example
make stop-example
```

HTTP endpoints are `http://localhost:18081`, `:18082` and `:18083`. The manifest
supplies two `worker` nodes and one `observer`; all three vote in `_meta`.
The example passes identities, peer addresses, credentials and a single
`StorageDir: "/data"` to the library. It uses `util.NewInstanceID()` for a fresh
process identity on every start. The library owns the fixed `/data/_meta.bolt`
and independent `/data/_authority.bolt` files; neither filename is configurable.
Retain the directory with the node identity. Old system filenames are refused
rather than silently treated as an empty directory; no migration is performed. It uses the default `topology.Limits`:
three nodes and five replicas per load group. Compose owns the three durable
volumes named `meta-node1`, `meta-node2` and `meta-node3`. These isolate the
small demo from volumes created by an earlier application. Stopping preserves
them; `make clean-example` explicitly deletes them.

For local development, use the example's module:

```sh
go -C examples/cluster run . serve --help
go -C examples/cluster test ./...
go -C examples/cluster build -o ../../bin/cluster .
```

`serve` is the default command. Configure it with flags or their corresponding
environment variables; explicit flags take precedence:

| Flag | Environment variable | Default |
| --- | --- | --- |
| `--node` | `GQUORUM_NODE` | Required |
| `--peers` | `GQUORUM_PEERS` | Required JSON array |
| `--credential` | `GQUORUM_CREDENTIAL` | Required |
| `--cluster` | `GQUORUM_CLUSTER` | `example` |
| `--data` | `GQUORUM_DATA` | `/data` |
| `--meta-listen` | `GQUORUM_META_LISTEN` | `:9000` |
| `--http-listen` | `GQUORUM_HTTP_LISTEN` | `:8080` |

Node types come from the peer manifest. The `healthcheck` command needs only a
URL and has a two-second timeout, configurable through `--timeout`:

```sh
go -C examples/cluster run . healthcheck http://localhost:18081/health/ready
```

The module uses `replace github.com/asalimonov/gquorum => ../..` to build against
the repository library. Root unit, functional, race and vet targets explicitly
include this module; the Compose smoke test lives in `tests/cluster`.

This demo creates a `gquorum.Node` from caller-supplied `NodeOptions`, calls
`Start` once, and uses `node.Cluster()` for topology operations. `Stop` joins
shutdown. The HTTP adapter checks Node readiness and approved node intent through the bounded progress read.
Load-group FSMs, protocols, application storage, WAL fault controls and cluster
scenarios live in `tests/cluster/testapp` and `tests/cluster`.

```sh
api=http://localhost:18081
curl -fsS "$api/api/cluster/status" | jq
curl -fsS "$api/api/cluster/status" | jq -r .meta_leader_node_name
curl -fsS "$api/api/progress?group=_meta" | jq

version=$(curl -fsS "$api/api/topology/latest" | jq -r .version)
result=$(curl -fsS -X POST "$api/api/nodes/node3/drain" \
  -H "If-Match: $version" -H 'Content-Type: application/json' \
  -d '{"actor":"operator","approve":true}')
operation=$(printf '%s' "$result" | jq -r .operation_id)
curl -fsS "$api/api/operations/$operation" | jq
```

`meta_leader_id` is the numeric `_meta` Raft member ID.
`meta_leader_node_name` gives its readable node name, such as `"node2"`.
Each group similarly reports numeric `leader_id` and string `leader_node_name`;
`host_node` is the reporting host. Names resolve through approved or proven identities.
An empty leader name means unknown or unmapped; the numeric ID can still be
available as a diagnostic.

Poll the operation until `state` is `succeeded`; `failed` and `superseded` are
terminal outcomes. A follower can briefly return 404 before observing the
operation. HTTP 202 confirms acceptance; inspect per-group progress before the next
edit. The approved node status becomes
`drained`, and its readiness endpoint returns 503. All three remain meta voters.
Reactivate it with a separately staged edit and approval:

```sh
version=$(curl -fsS "$api/api/topology/latest" | jq -r .version)
staged=$(curl -fsS -X POST "$api/api/nodes/node3/activate" \
  -H "If-Match: $version" -H 'Content-Type: application/json' \
  -d '{"actor":"operator","approve":false}')
version=$(printf '%s' "$staged" | jq -r .document.version)
operation=$(curl -fsS -X POST "$api/api/topology/$version/approve" | jq -r .operation_id)
curl -fsS "$api/api/operations/$operation" | jq
```

Concurrent/stale version edits return 409. Mutations require one positive
`If-Match` version, bare or quoted. `preview:true` previews an edit without
staging it and cannot combine with `approve:true`. The approval endpoint accepts
an empty body or `{}`, no query parameters, and an optional matching `If-Match`.
If staging commits but approval fails, `X-Staged-Version` identifies the document
to inspect and approve.

Routes: `GET /health/{live,ready}`, `GET /api/cluster/status`,
`GET /api/topology/{latest,target}`, `POST /api/nodes/{name}/{drain,activate}`,
`GET /api/progress?group={id}`, `POST /api/topology/{version}/approve`, and `GET /api/operations/{id}`.

`make test-example` runs the actual Compose demo from `tests/cluster/example_test.go`,
using a unique project/image, random host ports and private temporary volumes.
It checks bootstrap, node types, both approval flows, previews, drain/activate,
unchanged meta membership, readiness, stale-version refusal and restart from
retained volumes. `make test-function` runs the richer
in-process library integration tests under the cluster test tree. Docker Raft
correctness/fault scenarios use `make test-cluster`.
