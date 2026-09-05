package simtest

import "github.com/kudesn1k1/artel"

// Observation is what oracles see of one node after the run: canonical bytes
// for equality checks and the value as the subject renders it. Oracles
// receive only plain data — never the simulator or the core.
//
// Value is the twin of Node.Apply's op string: ops go in and the value comes
// out, both in the subject's own text vocabulary (a counter's decimal, a
// set's sorted elements). Family oracles parse it; reports print it. State,
// not Value, decides convergence — equal values do not imply equal states.
type Observation struct {
	Node  string
	State []byte
	Value string
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
