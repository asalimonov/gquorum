module lifecycle_test {
  import types.* from "./types"
  import basicSpells.* from "./vendor/spells/basicSpells"
  import lifecycle as L from "./lifecycle"
  import initial as I from "./initial"

  pure val doc = I::declarations(Map("n1" -> 1, "n2" -> 2, "n3" -> 3), Set("a", "b"))
  pure val node = I::initialNode("n1", doc, I::consensus(doc, "n1"))
  pure val replica = node.volatile.owners.get("a").key
  pure val first: RequestKey = { caller: { node: "n1", incarnation: 1 }, sequence: 1 }
  pure val second = { ...first, sequence: 2 }
  pure val reserved = L::reserved(replica, first, doc.groups.get("a"), 1)
  pure val opening = L::beginFactory(reserved)

  run concurrentStartsShareOneOwnerTest = {
    val empty = { ...node, volatile: { ...node.volatile, owners: Map() } }
    val one = L::reserve(empty, replica, first, true, doc.groups.get("a"), 1)
    val two = L::reserve(one, replica, second, true, doc.groups.get("a"), 1)
    and { L::canReserve(one, replica, second, true),
      two.volatile.owners.get("a").reservation == Some(first),
      two.volatile.owners.get("a").waiters == Set(first, second),
      two.volatile.owners.keys() == Set("a") }
  }
  run cancelledStartWaiterDoesNotCancelFactoryTest = {
    val cancelled = L::cancelWaiter(opening, first)
    val returned = L::factoryReturned(cancelled, true)
    and { cancelled.waiters == Set(), cancelled.factoryOutstanding,
      returned.applicationClose == Acquired, returned.phase == Replaying, returned.replayOutstanding,
      L::ownerSafety(cancelled), L::ownerSafety(returned) }
  }
  run lateFactorySuccessRetainsCloseOwnershipTest = {
    val waiting = L::requestStop(L::expireFactory(opening))
    val returned = L::factoryReturned(waiting, true)
    and { not(L::released(waiting)), not(L::canClose(waiting)),
      returned.phase == OwnerStopping, returned.applicationClose == Acquired,
      not(L::released(returned)), L::canClose(returned),
      not(L::ownerSafety(L::finishRelease(returned))) }
  }
  run failedFactoryDoesNotTransferPartialAcquisitionTest = {
    val failed = L::factoryReturned(opening, false)
    and { failed.applicationClose == NotAcquired, L::released(failed),
      not(L::canClose(failed)), L::ownerSafety(L::finishRelease(failed)) }
  }
  run failedClosePreventsReplacementTest = {
    val stopping = L::requestStop(node.volatile.owners.get("a"))
    val joined = L::joined(stopping)
    val closing = L::beginClose(joined)
    val failed = L::closeReturned(closing, false)
    val snapshot = { ...node, volatile: { ...node.volatile, owners: Map("a" -> failed) } }
    and { not(L::canClose(stopping)), not(L::released(stopping)),
      not(L::canClose(closing)), not(L::released(closing)),
      failed.applicationClose == CloseFailed, L::canClose(failed), not(L::released(failed)),
      not(L::canReserve(snapshot, replica, second, true)),
      not(L::canReserve(snapshot, { ...replica, replica: 4 }, second, true)),
      L::ownerSafety(failed) }
  }
  run explicitStartClearsOnlyMatchingHoldTest = {
    val old = { ...replica, replica: 9 }
    val held = L::hold(node, old)
    val started = L::reserve(held, replica, first, true, doc.groups.get("a"), 1)
    and { held.volatile.owners.get("a") == node.volatile.owners.get("a"),
      started.volatile.holds == Set(old), L::canReserve(held, replica, first, true) }
  }
  run earlyPublicationAndReleaseControlsTest = and {
    not(L::ownerSafety(L::publish(opening))),
    not(L::ownerSafety(L::finishRelease(L::requestStop(opening)))),
    not(L::canPublish(L::replayFinished(L::factoryReturned(opening, true)))),
    L::canPublish(L::authorize(L::replayFinished(L::factoryReturned(opening, true)))),
  }
}
