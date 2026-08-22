package simtest

import "github.com/kudesn1k1/artel"

// Observation is what oracles see of one node after the run: canonical bytes
// for equality checks and a human-readable value for reports. Oracles receive
// only plain data — never the simulator or the core.
type Observation struct {
	State []byte
	Human string
}

// Node is one running participant: a protocol core plus the subject-specific
// way to mutate and observe it.
type Node interface {
	Core() artel.Core
	// Apply performs one scenario op (subject-defined encoding, e.g. "inc:5").
	Apply(op string) error
	Observe() Observation
}

// Subject builds nodes for a run. NewNode returns a fresh incarnation — it is
// called for every node at scenario start (incarnation 1), and again on a
// restart with the incarnation bumped.
type Subject interface {
	NewNode(id string, incarnation int, peers []string) Node
}
