module sessions_test {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import initial as I from "../../harness/initial"
  import sessions as S from "../../metaengine/sessions"

  pure val meta = I::metadata(I::declarations(Map("n1" -> 1, "n2" -> 2, "n3" -> 3), Set("a")))
  pure val old: SessionKey = { owner: { node: "n2", incarnation: 1 }, generation: 1 }
  pure val fresh: SessionKey = { owner: { node: "n2", incarnation: 2 }, generation: 1 }
  pure val grant = GrantInternalSession({ key: fresh, member: 2 })

  run sessionGrantDoesNotOwnNodeNameTest = {
    val granted = S::apply(meta, grant)
    and { S::errors(meta, grant) == Set(), granted.activeSessions.get("n2") == old,
      S::errors(granted, BindInternalSession(fresh)) == Set(Conflict), granted.locks == Map() }
  }
  run staleSessionRevokeCannotRemoveSuccessorTest = {
    val replaced = S::apply(S::apply(S::apply(meta, RevokeInternalSession(old)), grant), BindInternalSession(fresh))
    val after = S::apply(replaced, RevokeInternalSession(old))
    and { S::metadataSafety(replaced), after == replaced, after.activeSessions.get("n2") == fresh,
      after.couriers.get("n2").process == fresh.owner }
  }
  run renewalRequiresExactProcessAndMemberTest = {
    val renewal: Renewal = { key: old, round: 1, member: 2 }
    and { S::renewalMatches(meta, renewal, old.owner),
      not(S::renewalMatches(meta, renewal, fresh.owner)),
      not(S::renewalMatches(meta, { ...renewal, member: 4 }, old.owner)),
      not(S::renewalMatches(S::apply(meta, RevokeInternalSession(old)), renewal, old.owner)) }
  }
  run sessionCannotActivateUnadmittedIdentityTest = and {
    S::errors(meta, GrantInternalSession({ key: fresh, member: 4 })) == Set(IdentityMismatch),
    S::errors(meta, GrantInternalSession({ key: { owner: { node: "n4", incarnation: 1 }, generation: 1 }, member: 4 })) == Set(IdentityMismatch),
  }
}
