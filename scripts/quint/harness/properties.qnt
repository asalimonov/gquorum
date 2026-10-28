module properties {
  import cleanup_properties as CleanupChecks from "./cleanup_properties"
  import managed_removal_steps as Removal from "../managed/removal_steps"
  import admission as Admission from "../metaengine/admission"
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import scope as S from "./scope"
  import lifecycle as Life from "../managed/lifecycle"
  import sessions as Sessions from "../metaengine/sessions"
  import grants as G from "../metaengine/grants"
  import execution as E from "../managed/execution"
  import authority as A from "../metaengine/authority"

  pure def initialScope(w: World): bool = and {
    S::documentInScope(w.extensions.metadata.latest), S::documentInScope(w.extensions.metadata.target),
    w.extensions.metadata.history.values().forall(d => S::documentInScope(d)),
    w.extensions.metadata.locks.keys().size() == 0, w.extensions.recovery.keys().size() == 0,
    w.extensions.storageConfig.values().forall(c => c.defaultBackend == OwnedWAL),
    w.system.values().forall(n => and {
      n.volatile.serveOverride == 0,
      S::documentInScope(n.volatile.localView.latest), S::documentInScope(n.volatile.localView.target),
      n.volatile.observations.values().forall(o => o.age == Fresh),
      n.durable.replicaEvidence.values().forall(e => match e { | ValidStore(_) => true | _ => false }),
    }),
    S::goodMetrics.lagWithinLimit and S::goodMetrics.backlogWithinLimit and S::goodMetrics.progressing and
      S::goodMetrics.current and S::goodMetrics.standbyStable and S::goodMetrics.cooldownClear,
  }

  pure def callsWellFormed(w: World): bool = w.system.keys().forall(n => {
    val owner = w.system.get(n)
    owner.volatile.pendingCalls.keys().forall(key => {
      val call = owner.volatile.pendingCalls.get(key)
      and {
        key.caller.node == n,
        key.sequence > 0,
        (call.phase == Replied) iff (call.result != None),
        match call.result {
          | Some(result) => match result {
            | Refused(reason) => (reason == Invalid and call.request == InvalidRequest) or
                (reason == Unavailable and call.request == ConfirmedRead) or
                (reason == WaitCancelled and call.waiterCancelled)
            | Observed(_) => call.request == ObserveLocal
            | ConfirmedView(reply) => and {
                call.request == ConfirmedRead, call.readIndex == Some(reply.index),
                reply.index <= reply.applied,
                w.extensions.metaLog.keys().contains(reply.applied),
                reply.view == { latest: w.extensions.metaLog.get(reply.applied).latest,
                  target: w.extensions.metaLog.get(reply.applied).target },
              }
            | Indeterminate => call.waiterCancelled and (call.command != None or call.operationRequest != None)
            | MutationResult(outcome) => and {
                match call.request { | Mutate(_) => true | Operate(_) => true | _ => false },
                match outcome.staged { | Some(v) => w.extensions.metadata.history.keys().contains(v) | None => true },
                call.stagedVersion == outcome.staged,
                match call.commitIndex { | Some(index) => owner.volatile.appliedMetaIndex >= index | None => true },
              }
            | Previewed(_) => match call.request { | Mutate(m) => m.options.preview | _ => false }
          }
          | _ => true
        },
      }
    })
  })

  pure def deletionDebtComplete(w: World): bool = w.extensions.deletionObligations.keys().forall(group =>
    w.extensions.metadata.retiredGroupIds.contains(group) and not(w.extensions.metadata.target.groups.keys().contains(group)) and
      w.extensions.deletionObligations.get(group).forall(key => w.extensions.metadata.cleanup.keys().contains(key) and
        w.extensions.metadata.cleanup.get(key).identity == key))

  pure def metadataWellFormed(w: World): bool = {
    val m = w.extensions.metadata
    and {
      E::physicalEvidenceValid(w), deletionDebtComplete(w), CleanupChecks::safety(w), Removal::removalSafety(w), Sessions::metadataSafety(m), Admission::metadataSafety(m),
      w.system.values().forall(n => n.volatile.owners.values().forall(owner => Life::ownerSafety(owner))),
      m.grants.values().forall(grant => G::wellFormed(grant)),
      m.authorityFloors.keys().forall(r => m.grants.keys().contains(m.authorityFloors.get(r)) and
        m.grants.get(m.authorityFloors.get(r)).resources.contains(r)),
      m.effectReceipts.forall(r => G::canRecordReceipt(m, r)),
      m.fenceEvidence.forall(f => G::canRecordFence(m, f)),
      m.revocations.keys().forall(k => m.grants.keys().contains(k) and m.closedGrants.contains(k) and
        m.revocations.get(k).provenResources.subseteq(m.revocations.get(k).requiredResources)),
      m.history.get(m.latest.version) == m.latest,
      m.target.version <= m.latest.version,
      m.history.get(m.target.version) == m.target,
      m.history.values().forall(d => d.nodes.values().forall(n => n.member < m.memberAllocationFloor)),
      m.stagedIntents.keys().forall(v => m.history.keys().contains(v)),
      m.firstAttempts.keys().forall(v => {
        val key = m.firstAttempts.get(v)
        key.version == v and key.attempt == 1 and m.operations.keys().contains(key)
      }),
      m.successors.keys().forall(previous => {
        val successor = m.successors.get(previous)
        if (not(m.operations.keys().contains(previous)) or not(m.operations.keys().contains(successor))) false else {
          val before = m.operations.get(previous)
          val after = m.operations.get(successor)
          and { successor.version == previous.version, successor.attempt == previous.attempt + 1,
            after.predecessor == Some(previous), before.plan == after.plan, before.audit == after.audit }
        }
      }),
      m.operations.keys().forall(key => {
        val op = m.operations.get(key)
        op.key == key and op.plan.version == key.version and m.history.keys().contains(key.version)
      }),
      m.groups.keys().forall(g => match m.groups.get(g).owner {
        | Some(key) => if (not(m.operations.keys().contains(key))) false else {
            val blocks = m.operations.get(key).blocks
            blocks.keys().contains(g) and not(A::terminal(blocks.get(g).phase))
          }
        | None => true
      }),
      m.retiredGroupIds.intersect(m.target.groups.keys()).size() == 0,
      m.cleanup.keys().forall(r => {
        val debt = m.cleanup.get(r)
        and { debt.identity == r, m.operations.keys().contains(debt.sourceOperation),
          match debt.stopAction {
            | Some(actionKey) => and { actionKey.step.target == ReplicaTarget(r), actionKey.step.kind == StopReplica,
                m.operations.keys().contains(actionKey.operation),
                m.operations.get(actionKey.operation).plan.cleanup.get(r.group)
                  .concat(m.operations.get(actionKey.operation).plan.blocks.getOrElse(r.group, List())).indices().exists(i =>
                    m.operations.get(actionKey.operation).plan.cleanup.get(r.group)
                      .concat(m.operations.get(actionKey.operation).plan.blocks.getOrElse(r.group, List())).nth(i) == actionKey.step) }
            | None => debt.stopped != None
          },
          match debt.retireAction { | Some(actionKey) => actionKey.step.targets.contains(r) | None => debt.retired != None },
        }
      }),
    }
  }

  pure def foundationHasNoEffects(w: World): bool = and {
    w.extensions.metadata.latest.version == 0,
    w.extensions.metadata.target.version == 0,
    w.extensions.metadata.grants.keys().size() == 0,
    w.extensions.metadata.cleanup.keys().size() == 0,
    w.system.keys().forall(n => and {
      w.system.get(n).durable.pin == None,
      w.system.get(n).durable.receipts.size() == 0,
      w.system.get(n).volatile.owners.keys().size() == 0,
    }),
  }

  pure def consensusWellFormed(w: World): bool = and {
    w.extensions.consensus.keys().contains("_meta"),
    w.extensions.consensus.keys().forall(g => {
      val c = w.extensions.consensus.get(g)
      and {
        c.voters.size() > 0,
        c.voters.intersect(c.learners).size() == 0,
        c.voters.union(c.learners).forall(r => r.group == g and r.cluster == c.lineage),
        c.appliedPositions.values().forall(index => index <= c.committedPosition),
        match c.leaderAuthority {
          | Some(leader) => c.voters.contains(leader)
          | None => true
        },
      }
    }),
    w.system.keys().forall(n => {
      val node = w.system.get(n)
      and {
        node.volatile.appliedMetaIndex <= w.extensions.consensus.get("_meta").committedPosition,
        node.durable.metaIndex <= w.extensions.consensus.get("_meta").committedPosition,
        node.volatile.incarnation >= 1,
        node.volatile.owners.values().forall(owner => owner.key.node == n),
      }
    }),
  }
}
