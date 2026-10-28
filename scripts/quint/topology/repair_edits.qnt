module repair_edits {
  import types.* from "../types"
  import basicSpells.* from "../vendor/spells/basicSpells"

  pure def shape(decision: RepairDecision): bool = match decision.kind {
    | RestoreMissingReplica => decision.group != "_meta" and decision.promote == None and decision.demote == None and
        (match decision.missing { | Some(key) => key.group == decision.group and key.replica > 0 | None => false })
    | ReconfigureVoters => decision.missing == None and (decision.promote != None or decision.demote != None) and
        (decision.promote == None or decision.demote == None or decision.promote != decision.demote) and
        (match decision.promote { | Some(key) => key.group == decision.group | None => true }) and
        (match decision.demote { | Some(key) => key.group == decision.group | None => true })
  }

  pure def roles(group: GroupDecl, decision: RepairDecision): GroupDecl = { ...group,
    replicas: group.replicas.map(r => if (decision.promote == Some(r.key)) { ...r, role: Voter }
      else if (decision.demote == Some(r.key)) { ...r, role: Learner } else r) }

}
