package simtest

import (
	"fmt"
	"slices"
	"strings"

	"github.com/kudesn1k1/artel"
	"github.com/kudesn1k1/artel/internal/causal"
)

// orsetNode drives one delta OR-Set replica through a core. Ops are "add:x"
// and "rm:x"; Value is the sorted elements, written "{a,b}". State is always
// the set's own JSON, whatever codec the core ships with: a codec mutant
// corrupts the wire, and Convergence judges the replicas as they are. A
// remove of an element the node does not hold changes nothing and ships
// nothing, so it is reported as an error: an op that produced no update is
// owed to nobody.
type orsetNode struct {
	id   string
	core artel.Core
	set  *causal.ORSet[string]
}

var _ Node = (*orsetNode)(nil)

func (n *orsetNode) Core() artel.Core { return n.core }

func (n *orsetNode) Apply(op string) error {
	kind, e, ok := strings.Cut(op, ":")
	if !ok || e == "" {
		return fmt.Errorf("simtest: invalid set op: %q", op)
	}
	switch kind {
	case "add":
		n.set.Add(e)
	case "rm":
		if !n.set.Contains(e) {
			return fmt.Errorf("simtest: %q is not in the set, nothing to remove", e)
		}
		n.set.Remove(e)
	default:
		return fmt.Errorf("simtest: invalid set op: %q", op)
	}
	return nil
}

func (n *orsetNode) Observe() Observation {
	state, err := causal.ORSetJSON[string]().Encode(n.set.State())
	if err != nil {
		panic(err)
	}
	return Observation{Node: n.id, State: state, Value: setValue(n.set.Elements())}
}

func setValue(elements []string) string {
	slices.Sort(elements)
	return "{" + strings.Join(elements, ",") + "}"
}

// orsetSubject runs the reference core over the delta OR-Set. It keeps every
// node it creates, in creation order, so a test can read refusal counts after
// a run; the replicas themselves carry nothing from one run to the next.
// codec is what the cores ship states with; nil means the set's own JSON.
type orsetSubject struct {
	codec artel.Codec[causal.ORSetState[string]]
	nodes []*orsetNode
}

var _ Subject = (*orsetSubject)(nil)

func (s *orsetSubject) NewNode(id string, incarnation int, peers []string) Node {
	codec := s.codec
	if codec == nil {
		codec = causal.ORSetJSON[string]()
	}
	set := causal.NewORSet[string](replicaID(id, incarnation))
	n := &orsetNode{id: id, core: newRefCore(id, set, peers, codec), set: set}
	s.nodes = append(s.nodes, n)
	return n
}

// rejections lists how many deltas each node refused, in creation order.
func (s *orsetSubject) rejections() []int {
	out := make([]int, len(s.nodes))
	for i, n := range s.nodes {
		out[i] = n.set.Rejected()
	}
	return out
}

// rejected is the refusals of every node the subject ever created.
func (s *orsetSubject) rejected() int {
	total := 0
	for _, n := range s.nodes {
		total += n.set.Rejected()
	}
	return total
}
