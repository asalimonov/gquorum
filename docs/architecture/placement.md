# Labels and placement

Status: **implemented**. This document describes the current feature contract.
The broader library context is in [library.md](library.md). Detailed design,
persistence fields, execution guards and acceptance criteria remain in the
[implementation specification](../brainstorms/20261013-labels-node-selector-target.md).
Decisions and source research are recorded in the
[brainstorm](../brainstorms/20261013-labels-node-selector.md).

## Ownership and purpose

Placement is enabled by default. Empty labels/selectors and an absent domain
constraint preserve the existing placement behavior. Applications declare
requirements; gquorum validates them and executes approved topology changes.
Applications own host-resource measurement and resource-informed policy.

| Data | Purpose |
| --- | --- |
| `topology.Node.Labels` | Node attributes such as `network=public_ip`, `az=az-a`, and the two library restriction keys below |
| `topology.Group.Labels` | Descriptive group metadata such as `purpose=accounting` or `tenant-tier=paid` |
| `PlacementRule.NodeSelector` | Explicit requirements on node labels for load replica hosting |
| `VoterConfig.DistinctVoterLabel` | Node-label key whose nonempty values must differ between voters |

Group labels do not select hosts, inherit from node labels, or trigger placement
rules. There is no implicit matching between these maps. A caller may use group
labels in its own controller, but must submit explicit placement changes through
the normal topology workflow. Editing descriptive group labels alone causes no
replica movement or voter rebalancing.

## Label schema

Node labels, group labels and selector maps use `map[string]string`. Each map
has at most 64 entries and 16,384 combined key/value bytes. Keys contain 1–253
UTF-8 bytes and no Unicode whitespace or control characters. Values contain
0–1,024 UTF-8 bytes and no Unicode control characters, including NUL. Invalid
UTF-8 is rejected. Matching is case-sensitive; no trimming or coercion occurs.
A present empty value is different from a missing key.

These restrictions apply to complete documents, including maps written directly
through exported fields. No legacy-label validation exception is introduced.

The `gquorum.io/` prefix is reserved. The only accepted keys under that prefix
are these node restrictions, each with exactly the value `"true"`:

| Constant | Key | Meaning |
| --- | --- | --- |
| `topology.LabelReserved` | `gquorum.io/reserved` | Block new load replica identities on this node |
| `topology.LabelLearnerOnly` | `gquorum.io/learner-only` | Forbid voting in every group, including `_meta` |

Delete the key to clear the restriction. Values `"false"` and `""` are invalid.
Group labels, selectors and the distinct-voter key cannot use the reserved
prefix. Ordinary keys, including `az` and `network`, belong to the caller;
gquorum defines no built-in values or automatic `Node.Region` fallback for them.

## Direct label API

Label operations belong to the topology structs. They do not add methods to
`Cluster`, the running `gquorum.Node`, or a local `load.Group` replica handle.

```go
func (n Node) GetLabels() map[string]string
func (n *Node) SetLabels(labels map[string]string) error

func (g Group) GetLabels() map[string]string
func (g *Group) SetLabels(labels map[string]string) error
```

`GetLabels` returns an owned, non-nil map, including for a zero-value struct.
Ordinary map lookup distinguishes absence from an empty value. Map iteration
order is unspecified. `SetLabels` replaces the entire set without merging:
keys omitted from the input are deleted, and nil or empty input clears all labels.
An empty set is stored as nil; GetLabels still returns a writable non-nil map.

SetLabels validates a copy of the full replacement before assigning it. Failure
leaves the struct unchanged; a nil receiver returns `fault.Invalid`. Even an
equal replacement is validated and does not retain a caller-owned map alias.
Neither mutating the getter result nor changing the setter input after return
changes the struct. A replacement on a shallow struct copy leaves the original
unchanged. Concurrent access to the same struct or mutation of setter input
during the call still requires caller synchronization.

Individual edits use GetLabels, Go map assignment/delete, and SetLabels. This
adds a few lines compared with dedicated per-key methods, but keeps the API
small and supports all four operations. Read the existing labels to preserve
other entries: passing only one key intentionally removes every other label.
This is a local read–modify–write operation, not an atomic remote patch.

The exported Labels map remains available for literals and bulk construction;
direct writes follow ordinary Go aliasing rules. All maps are copied at
ownership boundaries. Neither method hides service lookup, I/O, timestamp
changes, staging or approval. Persistent edits require the existing topology
CAS and approval workflow; local edits never change a running cluster by themselves.

## Selectors and inheritance

Selectors combine exact key/value equality with AND. A missing node key never
matches, even when the requested value is empty. There are no set, existence,
negation, regex, affinity, tolerance or soft-preference operators.

Load groups inherit `Document.Placement` when `Group.Placement == nil`. A
non-nil group placement replaces the **entire** rule, including replica bounds,
allowed node types and selector. Maps are not merged. Consequently, a complete
group override with an empty selector opts out of the inherited selector;
setting `Group.Placement` back to nil restores inheritance. Overrides must pass
the same complete-rule validation as document placement.

Host eligibility combines node state, allowed type, effective selector, explicit
hosting intent and reservation. `HostingAllEligible` places a replica on every
eligible node; it does not mean choose the best N nodes. Replica limits still
apply, including during transitions. The library never silently truncates a host
set to fit a limit. `_meta` retains its mandatory hosting behavior and ignores
load selectors and reservation.

## Reservation, voting and availability zones

Reservation preserves an existing exact load replica identity from the prior
approved `Target`, including an approved assignment still being prepared. A
staged-only assignment does not qualify. Restarting that identity is permitted
under ordinary recovery rules. Reseating or readmitting a fresh load replica ID
is a new assignment and is blocked. Reservation does not override other hosting
constraints and does not evacuate existing replicas; evacuation remains an
explicit drain or placement operation. Removing the marker can add learners to
all affected all-eligible groups and is included in preview.

Learner-only is a voting restriction for both loads and `_meta`. Adding it to a
voter requires a safe approved demotion/replacement plan. It does not overwrite
the separate `Replica.PinnedLearner` flag. Removing it permits later promotion
without automatically replacing healthy voters or clearing explicit pins. An
existing learner on a reserved node may be promoted if every other voting and
safety condition permits it.

`DistinctVoterLabel`, when nonempty, requires each voter to have a nonempty value
for that node-label key and permits at most one voter per value. Learners may
share domains or lack the label. This applies to load and `_meta` voter configurations.
For example, `DistinctVoterLabel: "az"` distributes voters across labeled AZs.

A fresh strict group requesting three voters with only two eligible AZs fails
with an unsatisfied-placement explanation. It does not create an under-target
group or relax the rule. Established groups retain quorum and catch-up guards;
repair may report a shortfall and wait. Labels describe operator intent and do
not prove node health or physical failure independence.

## Editing and approval

The topology package exposes pure helpers, with no additional Cluster methods:

```go
type PreparedPlacement struct {
    Document Document
    Report PlacementReport
}

func PreparePlacement(view ReadView, edited Document, rules Rules, at time.Time) (PreparedPlacement, error)
func ExplainPlacement(view ReadView, edited Document, rules Rules) (PlacementReport, error)
```

Preparation owns copies, reconciles affected hosts and voter roles, and returns
a complete desired document and structured explanation. It sets the next
version and audit timestamps using the supplied time; the caller supplies
`ChangedBy` and `Comment`. Any allocated replica IDs remain tentative until
staging. Explanation evaluates the same proposed placement without publishing a
document or IDs. Neither helper probes native Raft membership or persists state.

Example using the proposed API; error handling returns to the caller at each
step, and the review step must inspect the complete document and plan:

```go
view, err := cluster.ReadTopology(ctx)
if err != nil { return err }
edited := view.Latest.Clone()
n := edited.Node("node-a")
if n == nil { return fmt.Errorf("unknown node") }
labels := n.GetLabels()
labels["network"] = "public_ip"
delete(labels, topology.LabelReserved)
if err := n.SetLabels(labels); err != nil { return err }
edited.ChangedBy = "operator"
edited.Comment = "open node-a for public groups"

prepared, err := topology.PreparePlacement(view, edited, cluster.Rules(), time.Now().UTC())
next, report := prepared.Document, prepared.Report
if err != nil { return err } // report carries placement explanations
plan, err := cluster.PlanFor(ctx, next)
if err != nil { return err }
// Present next, report and plan for review before staging/approval.
_, _ = report, plan
staged, err := cluster.Stage(ctx, next, view.Latest.Version)
if err != nil { return err }
_, err = cluster.Approve(ctx, staged.Version)
// Preserve staged.Version if approval fails, for explicit retry or replanning.
return err
```

Use the same label methods on `edited.Group(id)` for group metadata. Assign
`Group.Placement.NodeSelector` explicitly for hosting requirements and
`Group.VoterConfig.DistinctVoterLabel` for voter spread; check the optional
pointers or construct complete validated rules first. `CreateGroup` accepts
initial labels through `GroupSpec.Labels`.

Preparation rejects conflicting manual replica/allocator edits to affected
groups. Expert callers may construct a complete desired document and use the
existing `PlanFor`/`Stage` path directly, with full validation and no implicit
materialization. The metadata FSM always revalidates against its own current
views; a caller-supplied view is not approval evidence. CAS conflicts require an
explicit reread/replan. Approval failures preserve a successfully staged version.

Constraint changes include required relocation in the reviewed plan. The
existing executor normally prepares a learner first, then checks catch-up,
guarded role changes and retirement with immutable grants and receipts. At a
hard native replica limit, it may remove a departing exact identity first only
when the intermediate live quorum and placement remain safe. Unsafe transitions
fail with an explanation. Existing replicas continue according to applied
authority during convergence; editing a label never triggers an immediate local
shutdown or role switch.

The built-in scheduler remains a repair policy, using the same placement checks.
It does not become a healthy-group balancer or approve unrelated operator intent.
There is no new placement controller, label service, cache or resource collector.

## Development and compatibility

Breaking changes are accepted at this development stage. This feature introduces
no storage migration, old-format replay branch, placement-revision field or
mixed-version negotiation. A metadata format version identifies the new supported
encoding; unsupported persisted state is refused unchanged. Current-format
restart/replay remains required. Fresh development state is an explicit caller
decision; the library never deletes incompatible state automatically.

The implementation specification records exact format changes. Existing
immutable compatibility fixtures were not regenerated to hide a format change.
Current and rejected-format verification is recorded in
[placement validation](../testing/placement-validation.md).
