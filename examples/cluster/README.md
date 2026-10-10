# Three-node cluster example

```sh
make run-example
```

HTTP endpoints are `http://localhost:18081`, `:18082` and `:18083`. `node1` and
`node2` are workers; `node3` is an observer. All three vote in `_meta`. The `demo`
load group allows all types with placement 2..3; `workers` permits only workers
with placement 1..2. Both groups use a full-state key/value machine. The example's
load protocol and flags/environment parsing are application code.

Compose gives each node a separate durable volume. The application passes
`/data/meta.bolt`, `/data/load` and `/data/events.bolt` explicitly to library
constructors. `make stop-example` preserves these volumes. `make clean-example`
explicitly removes the example volumes. Restarting preserves cluster identity
and load data and generates a new process identity.

The commands below use `curl` and `jq`:

```sh
api=http://localhost:18081
curl -fsS "$api/api/topology/active" | jq
curl -fsS "$api/api/cluster/status" | jq

# An observer can forward a worker-group write to the exact serving leader.
curl -fsS -X PUT http://localhost:18083/api/load/workers/keys/message \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"write-message-1","value":"hello"}'
curl -fsS "$api/api/load/workers/keys/message" | jq
```

Writes require a request ID. Repeating the same ID and command succeeds without
reapplying the write; using that ID for another command returns 409. Request
fingerprints are included in snapshots. Reads return this node's locally applied
state and explicitly report `consistency: local`. An observer cannot locally read
the worker-only group and receives 503.

Mutations use the observed Latest version in `If-Match`. Staging and approval
are separate operations; `apply: true` requests both. A successful approval
returns 202 and an execution ID, whose durable status can be polled:

```sh
version=$(curl -fsS "$api/api/topology/latest" | jq -r .version)
result=$(curl -fsS -X POST "$api/api/nodes/node3/drain" \
  -H "If-Match: $version" -H 'Content-Type: application/json' \
  -d '{"actor":"operator","apply":true}')
execution=$(printf '%s' "$result" | jq -r .execution_id)
curl -fsS "$api/api/executions/$execution" | jq
```

Poll until `state` is `succeeded` before the next edit. Draining the observer
reduces `demo` to two voters, leaves `workers` unchanged and preserves all three
meta voters. The drained node's readiness endpoint returns 503. Reactivation
allocates a fresh load replica ID and restores state from the retained peers:

```sh
version=$(curl -fsS "$api/api/topology/latest" | jq -r .version)
curl -fsS -X POST "$api/api/nodes/node3/activate" \
  -H "If-Match: $version" -H 'Content-Type: application/json' \
  -d '{"actor":"operator","apply":true}' | jq
```

The fixed membership safety policy refuses shrinking a two-voter group to one.
Consequently draining a worker returns 422 at approval. Staging may already have
committed: `X-Staged-Version` identifies the edit, Latest retains it, and Target
and Active retain their approved state. A min-two worker placement in that staged
one-active-worker document also returns 422. Restore the approved document with
`PUT /api/topology` and the current Latest `If-Match`, then plan/apply its returned
version. Do not repeat the drain blindly or treat the refusal as rollback.

Group creation uses this body with `POST /api/groups` and `If-Match`:

```json
{"group":{"id":"another","placement":{"min_replicas":1,"max_replicas":2,"allowed_node_types":["worker"]}},"mutation":{"actor":"operator","apply":true}}
```

Use `PUT /api/groups/{id}/placement` with
`{"placement":{...},"mutation":{"actor":"operator","apply":true}}` to edit
one group's desired policy. `DELETE /api/groups/{id}` takes mutation options and
`If-Match`; approved deletion permanently retires the ID. Recreating the workload
requires a new group ID. Immediate leadership transfer uses
`POST /api/groups/{id}/leader/{name}`.

Other routes:

- `GET /health/live`, `/health/ready`
- `GET /api/topology/{latest,target,active}`, `/api/topology/history?offset=0&limit=100`
- `PUT /api/topology` with an explicit document and `If-Match`
- `POST /api/topology/{version}/plan`, `/api/topology/{version}/apply`
- `GET /api/executions/{id}`, `/api/cluster/status`
- `POST /api/nodes` with `{"node":{...},"mutation":{...}}`
- `POST /api/nodes/{name}/{drain,activate,exclude}` with mutation options
- `GET /api/events?after=0&limit=100&group=demo&type=leader_change`
- `GET /api/events/watch?group=demo` (live SSE)

Events and sequence cursors are local to each node. SSE includes `lost_before`
for delivery gaps; sequence zero denotes an event not persisted to the journal.
Watch is live-only: replay uses the paged journal API, not Last-Event-ID.

Drain the old node first so its load cleanup completes before actual meta
removal triggers process shutdown. Replacement requires exclusion of the old
node before admitting its replacement
under the default three-node limit. A joining process supplies `-join` and a peer
manifest with seed meta addresses. Old retained load identities are stopped and
verified before scoped quarantine; a readmission callback verifies the closed
meta pin before moving the supplied database. Unknown or foreign storage is
refused rather than erased.

`make test-example` exercises the real HTTP, meta gRPC and application load gRPC
servers. It checks bootstrap, typed placement, replicated/idempotent writes,
CAS conflicts, drain/activate, unsafe shrink and placement refusal, staged-edit
recovery, deletion/retirement, leadership transfer, SSE/journal ordering, persisted restart,
and exclusion followed by fresh-ID readmission of a retained node. It skips under short mode;
the example's isolated unit tests run in parallel through `make test-unit`.
