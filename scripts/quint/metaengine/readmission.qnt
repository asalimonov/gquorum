module readmission {
  import types.* from "../types"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import readmission_base as B from "./readmission_base"
  import admission as A from "./admission"
  import grants as G from "./grants"

  pure def admissionKey(a: ProcessAdmission, previous: int): AdmissionKey = {
    node: a.owner.node, predecessor: previous, successor: a.replica.member,
  }

  pure def beginErrors(meta: MetadataState, a: ProcessAdmission, previous: int): Set[Refusal] = {
    val key = admissionKey(a, previous)
    val grants = B::batch(meta, key)
    if (previous <= 0 or previous >= a.replica.member or not(A::current(meta, a)) or
        not(meta.admissions.keys().contains(a.owner.node)) or not(A::same(meta.admissions.get(a.owner.node), a))) Set(Unauthorized)
    else if (grants.size() > 0) {
      if (grants.forall(g => G::currentForIssuance(meta, g))) Set() else Set(Unauthorized)
    } else if (previous != B::previousMember(meta, a.owner.node, a.replica.member)) Set(IdentityMismatch)
    else {
      val targets = B::priorTargets(meta, key)
      if (not(targets.exists(r => r.group == "_meta"))) Set(Unauthorized)
      else if (not(G::resourcesAvailable(meta, targets.map(r => HostResource({ group: r.group, node: r.node }))))) Set(Unavailable)
      else Set()
    }
  }

  pure def begin(meta: MetadataState, a: ProcessAdmission, previous: int, revision: int): MetadataState = {
    val key = admissionKey(a, previous)
    if (B::batch(meta, key).size() > 0) meta else {
      val targets = B::priorTargets(meta, key)
      val granted = targets.map(r => r.group).fold(meta, (current, group) => G::install(current, {
        key: { cluster: meta.target.cluster, group: group, revision: revision }, purpose: Readmission,
        effect: None, admission: Some(key), resources: Set(HostResource({ group: group, node: key.node })),
        targets: targets.filter(r => r.group == group), planStep: None, request: None }))
      { ...granted, readmissionOrigins: granted.readmissionOrigins.put(key,
          match a.origin { | Some(origin) => origin | None => a.operation }) }
    }
  }

  pure def recordErrors(meta: MetadataState, a: ProcessAdmission, receipts: Set[Receipt]): Set[Refusal] = {
    if (not(meta.admissions.keys().contains(a.owner.node)) or not(A::same(meta.admissions.get(a.owner.node), a)) or receipts.size() == 0)
      Set(Unauthorized)
    else if (not(receipts.forall(r => meta.grants.keys().contains(r.grant) and
        meta.grants.get(r.grant).purpose == Readmission and G::canRecordReceipt(meta, r) and
        (match r.admission { | Some(key) => key.node == a.owner.node and key.successor == a.replica.member and
            meta.readmissionOrigins.keys().contains(key) | None => false })))) Set(Unauthorized)
    else Set()
  }

  pure def record(meta: MetadataState, receipts: Set[Receipt]): MetadataState =
    B::settle(receipts.fold(meta, (current, receipt) => G::recordReceipt(current, receipt)))
}
