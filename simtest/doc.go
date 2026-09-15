// Package simtest tests replicated data types and the protocols that ship
// them against an adversarial network, and proves its own verdicts can be
// trusted. It is experimental until 1.0: names and shapes may change between
// minor releases.
//
// # The simulator
//
// A [Scenario] is plain data: a node count, a topology, a gossip interval,
// the ops each node applies and when, and the anomalies in force over which
// windows ([FaultEntry]): drops, delays, duplicates, partitions, lost acks
// and, for experiments only, a transport that reports success without
// delivering. [Run] plays it over a [Subject], which builds one [Node] per
// replica: a sans-IO protocol core plus the subject's own way to apply an op
// and to observe the replica. The scheduler owns the clock and the wire, so
// a run is a function of its scenario alone: the same scenario yields the
// same [Trace] byte for byte, and [RequireDeterministic] checks exactly that.
//
// # Oracles
//
// An [Oracle] judges a run from plain data, the [History] distilled from the
// trace and one [Observation] per node, never from the simulator or the
// cores. [Convergence] checks that every live node holds the same state,
// [CounterSum] that a counter subject lost or duplicated nothing,
// [EventualDelivery] that every update had a carrier to every node it was
// owed to, judged against the dissemination pattern the subject claims. An
// oracle that cannot read its input reports that as a violation, so silence
// always means "checked and clean".
//
// # Modes
//
// [GenScenario] draws a scenario from a seed within a [Profile]. [Stress]
// runs many of them and returns the failing ones with their seeds; [Shrink]
// cuts a failing scenario down to a smaller one that still fails the same
// oracles. A trace prints as swim lanes with [Trace.String] and exports as
// JSON lines with [Trace.WriteJSONL].
//
// # Live engines
//
// The simulator runs protocol cores, not engines. To put a running engine
// under the same anomalies, wrap its transport in [Chaos]: the engine keeps
// its goroutines, timers and workers, and only the network misbehaves.
// Reproducibility there is statistical, since the seed fixes the anomaly
// decisions but not the goroutine schedule, so assertions on live engines
// are eventual, and byte-for-byte replay stays with the simulator.
package simtest
