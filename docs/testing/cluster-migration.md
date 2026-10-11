# Multi-node runtime test migration

Multi-node scenarios now run in `tests/cluster` with `//go:build clustertest`,
through `make test-cluster` and the existing bounded Docker pool. They use real
application processes, gRPC, private networks and durable volumes. Cases and
independent admission-phase subtests run in parallel, with `CLUSTER_PARALLEL=8` by default. Every lease has fresh identities, credentials, containers and disks;
release verifies cleanup before the network can be reused.

The migration merges duplicate scenarios into existing process coverage and adds
11 top-level process tests for missing runtime contracts. The functional suite
keeps single-participant and isolated storage/protocol/callback checks. Its meta
and public-node helpers no longer accept a participant count.

## Coverage map

Names below identify the former functional tests or families. Process files
contain the existing or newly ported assertions. Ordinary scenarios run at three
nodes; four-node admission cases target learner promotion, leadership changes and
durable pending/ready pin recovery. Generic five/seven-node repetitions were removed.

| Former functional scenario | Process coverage |
| --- | --- |
| Meta engine real gRPC consensus, bbolt restart and leader replacement; public founding activation | [meta_test.go](../../tests/cluster/meta_test.go), [founding_test.go](../../tests/cluster/founding_test.go) |
| Load three-node consensus and leader replacement | [meta_test.go](../../tests/cluster/meta_test.go), [load_test.go](../../tests/cluster/load_test.go), [recovery_test.go](../../tests/cluster/recovery_test.go) |
| Load membership learner promotion, transfer and removal | [growth_test.go](../../tests/cluster/growth_test.go), [api_contract_test.go](../../tests/cluster/api_contract_test.go), [membership_test.go](../../tests/cluster/membership_test.go) |
| Managed founders replicate and resume through snapshot delivery | [growth_test.go](../../tests/cluster/growth_test.go): compacted learner receives the complete application snapshot and acknowledged commands |
| Approval requires exact response; forwarding preserves requested version and does not recurse | [meta_protocol_test.go](../../tests/cluster/meta_protocol_test.go): actual follower API/RPC calls; malformed or empty success replies remain isolated in [approval_test.go](../../internal/metaservice/approval_test.go), and domain-error encoding in [refusal_test.go](../../internal/metatransport/refusal_test.go) |
| Join needs approval and a prepared exact runtime before learner membership | [join_test.go](../../tests/cluster/join_test.go), [topology_test.go](../../tests/cluster/topology_test.go) |
| Retained joining disk and copied live identity are fenced; parked refusal is visible on followers | [rejoin_test.go](../../tests/cluster/rejoin_test.go), [identity_test.go](../../tests/cluster/identity_test.go) |
| Data-loss readmission retries the same grant and retires former hosts | [recovery_test.go](../../tests/cluster/recovery_test.go), [rejoin_test.go](../../tests/cluster/rejoin_test.go); hook refusal/ownership checks remain isolated in `internal/managed` |
| Pinned admission restarts before and after ready without allocating IDs | [join_test.go](../../tests/cluster/join_test.go): protocol-driver process is killed after durable pinning, optionally after `JoinReady`; the ordinary SDK replacement retains the member, document and operator attempt |
| Caller effect completion is lost across leader replacement | [runtime_test.go](../../tests/cluster/runtime_test.go): hold acknowledgement after founding effects, abruptly kill the meta leader, finish the same attempt and verify one factory acquisition per surviving host |
| Load group create/delete and retired IDs | [api_contract_test.go](../../tests/cluster/api_contract_test.go), [scale_test.go](../../tests/cluster/scale_test.go), [execution_test.go](../../tests/cluster/execution_test.go) |
| Transfer meta leadership from a follower using Active scope | [api_contract_test.go](../../tests/cluster/api_contract_test.go), [execution_test.go](../../tests/cluster/execution_test.go) |
| Follower empty-plan approval durably activates documents; follower CAS and owned history | [api_contract_test.go](../../tests/cluster/api_contract_test.go), [cas_test.go](../../tests/cluster/cas_test.go), [watch_test.go](../../tests/cluster/watch_test.go) |
| Health/status/events and drain use approved Active scope | [health_recovery_test.go](../../tests/cluster/health_recovery_test.go), [membership_test.go](../../tests/cluster/membership_test.go), [watch_test.go](../../tests/cluster/watch_test.go) |
| Drain transfers leadership and retains admitted meta voters | [membership_test.go](../../tests/cluster/membership_test.go), [api_contract_test.go](../../tests/cluster/api_contract_test.go) |
| Self-removal is delivered through durable application and fences before replaceable policy | [runtime_test.go](../../tests/cluster/runtime_test.go), [removal_store_test.go](../../tests/cluster/removal_store_test.go): check the fence inside both policy calls, retry once, stop the real participant, inspect closed bbolt membership, exclusion and applied index |
| Automatic repair and operator reseat use fresh exact identities | [health_recovery_test.go](../../tests/cluster/health_recovery_test.go), [recovery_test.go](../../tests/cluster/recovery_test.go) |
| Exact IdentityPing/KeepAlive; activation, deactivation, locks and delayed revocation | [meta_protocol_test.go](../../tests/cluster/meta_protocol_test.go), [runtime_test.go](../../tests/cluster/runtime_test.go) |
| Watch fanout is owned and durable on followers | [watch_test.go](../../tests/cluster/watch_test.go): subscribe to all three views on every host, verify staged/approved scope, kill and restart each follower after delivery and check retained content and identities |
| Slow destination cannot delay the healthy majority; shutdown joins senders | [runtime_test.go](../../tests/cluster/runtime_test.go): pause the actual peer, acknowledge 50 bounded writes on the majority, recover every command and require clean process exits; outbound FIFO/worker-retirement assertions remain in `internal/raftengine` |
| Public node sequential startup, explicit local holds and retained recovery | [runtime_test.go](../../tests/cluster/runtime_test.go), [founding_test.go](../../tests/cluster/founding_test.go); the local nonblocking/repeated `Start` contract uses one real participant with absent peers in `testapp/node_lifecycle_api_func_test.go` |
| All-founders gate preserves protocol availability; founding timeout is not repeated on retained restart | [founding_test.go](../../tests/cluster/founding_test.go) |
| Caller schedulers follow leadership without agreement gate | [runtime_test.go](../../tests/cluster/runtime_test.go): distinct per-node callbacks, leadership transfer, unchanged topology and disable/enable controls |
| Concurrent authorized cleanup retries are idempotent | [runtime_test.go](../../tests/cluster/runtime_test.go): 32 HTTP calls submit the same exact authorized local action while the primary callback is held; verify store removal and operator convergence |
| Fixture REST drain, activate, retirement and retained readmission | [api_contract_test.go](../../tests/cluster/api_contract_test.go), [rejoin_test.go](../../tests/cluster/rejoin_test.go), [execution_test.go](../../tests/cluster/execution_test.go), [recovery_test.go](../../tests/cluster/recovery_test.go) |
| Fixture live leader event follows journal append | [watch_test.go](../../tests/cluster/watch_test.go): actual filtered SSE subscription, transferred leader, sequence/loss checks and immediate journal lookup |

## Retained component checks

Functional tests still exercise one local Raft participant, callback ownership,
quiescence, replay, protocol doubles, exact authorization, store failure handling
and WAL durability. They do not assemble a communicating multi-node cluster.
Single-node approval failure still proves that a durable staged document is
returned without changing Target or Active. Local meta status still proves it
can be read while the caller controller is locked and returns owned membership.

The quiescence regression waits for the callback to acknowledge cancellation
under its existing deadline. It still requires shutdown to retain ownership
until the callback actually returns.

No runtime test was executed during this migration. Static verification covers
unit, functional and `clustertest` builds with `go vet`; the new Docker scenarios
still need execution after rebuilding the fixture image.
