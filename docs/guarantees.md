# Guarantees

What artel promises today, what it will promise later, and what it will never
promise. Wording here is normative: if the library violates a "guaranteed"
item, that is a bug — please report it.

> **Engine status:** the current engine runs an *interim* anti-entropy protocol
> (full-mesh, direct gossip). It already delivers the guarantees below; the
> target protocol (delta-intervals with causal delivery guarantees) replaces it
> before 1.0 and adds causal consistency to this list.

## Guaranteed today

- **Convergence (Strong Eventual Consistency).** Replicas that have received
  the same set of updates — in any order, with any duplication — hold equal
  state. Conflict resolution is a join-semilattice merge: commutative,
  associative, idempotent.
- **No lost updates.** A locally accepted mutation reaches every reachable
  peer. Deltas coalesce by joining, which loses nothing: a coalesced delta
  carries exactly the same information as the sequence it replaced.
- **Determinism.** Conflicts resolve identically on every replica, and never
  by wall-clock time.
- **Thread-safety.** Types are safe for concurrent use. `State()` and
  `Delta()` return snapshots that later mutations do not affect.
- **Liveness needs only an eventually-connected network.** Message loss,
  reordering, duplication and temporary partitions delay convergence but never
  corrupt state: safety does not depend on the network behaving.

## Not yet guaranteed

- **Causal consistency.** With the interim protocol, a replica may observe a
  later update before an earlier one from the same origin (harmless for the
  current counter types, which is why they ship first). Arrives with the
  target protocol.
- **Durability.** There is no persistence yet: a restarted process recovers
  state by pulling from live peers. If every replica is down at once,
  unreplicated updates are lost. Persistence is a scheduled pre-1.0 milestone.
- **Wire-format stability.** Payload encodings may change between 0.x
  releases without a migration path.

## Never promised

- History, time-travel, or blame — artel stores where the state *is*, not how
  it got there. This is a design decision, not a missing feature.
- Byzantine fault tolerance: replicas are trusted; a malicious peer can
  corrupt state.
- Authentication or encryption in the core — put them in the transport (the
  `Transport` interface is the seam; wrap or replace it).
- Ordering across independent types: two CRDTs replicated side by side
  converge independently, with no cross-type ordering relationship.
- Cross-language wire compatibility.
