package simtest

import (
	"bytes"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/kudesn1k1/artel"
)

// History is the trace distilled for oracles: plain data about one run with
// the scheduler's vocabulary left behind. Run derives it from the Trace; a
// test over live engines builds it by hand and calls Check directly.
type History struct {
	Nodes      []string   // the run's nodes
	Ops        []Op       // accepted ops only: an op the subject refused is owed to nobody
	Deliveries []Delivery // in trace order (ascending Seq) — the relay closure relies on it
	Crashed    []string   // dead at the END of the run; what they did while alive stands
}

// Op is one accepted local mutation: Node applied Op at trace row Seq.
type Op struct {
	T        Dur
	Seq      uint64
	Node, Op string
}

// Delivery is one message handed to a node: To received a Kind ("push" or
// "pull") from From at trace row Seq; Sent is the row of the send it came
// from. A push can carry state only if it was sent after that state existed,
// so the oracles order by Sent — never by T, which cannot order rows within
// one instant.
type Delivery struct {
	T              Dur
	Seq, Sent      uint64
	From, To, Kind string
}

// Violation is one verdict: Oracle is the reporting oracle's Name, Detail
// names the nodes and values involved.
type Violation struct {
	Oracle, Detail string
}

// Oracle judges a run from plain data — a test oracle in the software-testing
// sense: it decides whether the observed outcome is correct, never how the
// run got there. Check receives the History and one Observation per node
// (matched by Observation.Node) and returns every violation it can attest
// to. An oracle that cannot read its input reports that as a violation rather
// than staying silent, so silence always means "checked and clean".
type Oracle interface {
	Name() string
	Check(h History, final []Observation) []Violation
}

type convergenceOracle struct{}

var _ Oracle = (*convergenceOracle)(nil)

func (o convergenceOracle) Name() string {
	return "convergence"
}

func (o convergenceOracle) Check(h History, final []Observation) []Violation {
	if len(h.Nodes) == 0 {
		return []Violation{}
	}

	if len(h.Nodes) != len(final) {
		panic(fmt.Sprintf("simtest: got %d observations for %d nodes", len(final), len(h.Nodes)))
	}

	crashed := make(map[string]struct{}, len(h.Crashed))
	for _, v := range h.Crashed {
		crashed[v] = struct{}{}
	}

	violations := make([]Violation, 0, len(h.Nodes))
	first, ok := findFirstLiveNode(final, crashed)
	if !ok {
		return violations
	}

	for _, f := range final {
		if _, ok := crashed[f.Node]; ok {
			continue
		}
		if !bytes.Equal(first.State, f.State) {
			violations = append(violations, Violation{Oracle: o.Name(), Detail: fmt.Sprintf("%s state mismatches %s. got: %s, want: %s", f.Node, first.Node, f.Value, first.Value)})
		}
	}

	return violations
}

// Convergence checks strong eventual consistency: every live node's State is
// byte-equal to every other's. It compares State, never Value — equal values
// do not imply equal states. Dead nodes are not compared; with no live node
// there is nothing to compare and the oracle is silent.
func Convergence() Oracle {
	return convergenceOracle{}
}

type counterSumOracle struct{}

var _ Oracle = (*counterSumOracle)(nil)

func (c counterSumOracle) Name() string {
	return "counter_sum"
}

func (c counterSumOracle) Check(h History, final []Observation) []Violation {
	if len(h.Nodes) == 0 {
		return []Violation{}
	}

	if len(h.Nodes) != len(final) {
		panic(fmt.Sprintf("simtest: got %d observations for %d nodes", len(final), len(h.Nodes)))
	}

	crashed := make(map[string]struct{}, len(h.Crashed))
	for _, v := range h.Crashed {
		crashed[v] = struct{}{}
	}

	violations := make([]Violation, 0, len(h.Nodes))

	expected := 0
	for _, o := range h.Ops {
		parsedOp, err := parseCounterOp(o.Op)
		if err != nil {
			violations = append(violations, Violation{Oracle: c.Name(), Detail: fmt.Sprintf("error parsing op: %s", err)})
			return violations
		}

		expected += parsedOp
	}

	for _, o := range final {
		if _, ok := crashed[o.Node]; ok {
			continue
		}
		observed, err := strconv.Atoi(o.Value)
		if err != nil {
			violations = append(violations, Violation{Oracle: c.Name(), Detail: fmt.Sprintf("error parsing observed value for node %s, got: %s", o.Node, o.Value)})
			continue
		}
		if expected != observed {
			violations = append(violations, Violation{Oracle: c.Name(), Detail: fmt.Sprintf("%s node value does not match expected. got: %d, expected: %d", o.Node, observed, expected)})
		}
	}

	return violations
}

// CounterSum checks a counter subject for lost or duplicated updates: the sum
// of the accepted ops, written "inc:N" or "dec:N", equals every live node's
// Value read as a decimal integer. Its reference point is outside the replicas — the op log —
// so it catches a wrong value the replicas agree on, which Convergence
// cannot. An op outside the vocabulary makes the expected sum unknowable:
// that is reported and no value is judged; a Value that is not a number is
// reported for its node. Dead nodes are not read.
func CounterSum() Oracle {
	return counterSumOracle{}
}

// Liveness is the strength of EventualDelivery: whose updates are owed to
// whom once nodes die.
type Liveness int

const (
	// OriginAlive: every update of a live origin reaches every live node.
	// Updates of dead origins are owed to nobody — the direct-mesh engine
	// meets this.
	OriginAlive Liveness = iota
	// AnySurvivor adds: an update that reached at least one live node reaches
	// every live node — the property of relaying protocols; a direct-only
	// protocol cannot meet it once an origin dies mid-dissemination.
	AnySurvivor
)

func (l Liveness) String() string {
	switch l {
	case OriginAlive:
		return "origin alive"
	case AnySurvivor:
		return "any survivor"
	default:
		return "unknown"
	}
}

// Pattern is the dissemination the subject claims; EventualDelivery holds it
// to that claim.
type Pattern int

const (
	// Direct: the origin itself pushes to every peer. An update reached y if
	// a push from the origin to y was sent after the op.
	Direct Pattern = iota
	// Relay: a causal chain suffices. An update reached y if pushes chained
	// from the origin to y, each sent after its sender learned the update.
	Relay
)

func (p Pattern) String() string {
	switch p {
	case Direct:
		return "direct"
	case Relay:
		return "relay"
	default:
		return "unknown"
	}
}

type deliveryOracle struct {
	Liveness Liveness
	Pattern  Pattern
}

var _ Oracle = (*deliveryOracle)(nil)

func (d deliveryOracle) Name() string {
	return fmt.Sprintf("delivery/%s/%s", d.Liveness.String(), d.Pattern.String())
}

func (d deliveryOracle) Check(h History, final []Observation) []Violation {
	var violations []Violation

	for _, op := range h.Ops {
		reached := d.reached(op, h)
		required := d.required(op, h, reached)

		missing := make([]string, 0, len(required))
		for req := range required {
			if _, ok := reached[req]; !ok {
				missing = append(missing, req)
			}
		}

		if len(missing) > 0 {
			slices.Sort(missing)
			violations = append(violations, Violation{
				Oracle: d.Name(),
				Detail: fmt.Sprintf("operation %s (@t=%d) from node %s missing for %s", op.Op, op.T, op.Node, strings.Join(missing, ", ")),
			})
		}
	}

	return violations
}

func (d deliveryOracle) reached(op Op, h History) map[string]struct{} {
	if d.Pattern == Direct {
		reached := make(map[string]struct{}, len(h.Nodes))
		reached[op.Node] = struct{}{}

		for _, del := range h.Deliveries {
			if del.Kind != artel.KindPush.String() || del.From != op.Node || del.Sent <= op.Seq {
				continue
			}
			reached[del.To] = struct{}{}
		}
		return reached
	}

	learnedAt := make(map[string]uint64, len(h.Nodes))
	learnedAt[op.Node] = op.Seq
	for _, del := range h.Deliveries {
		if del.Kind != artel.KindPush.String() {
			continue
		}
		if _, ok := learnedAt[del.From]; !ok {
			continue
		}
		if del.Sent <= learnedAt[del.From] {
			continue
		}

		if _, ok := learnedAt[del.To]; !ok {
			learnedAt[del.To] = del.Seq
		}
	}

	reached := make(map[string]struct{}, len(learnedAt))
	for k := range learnedAt {
		reached[k] = struct{}{}
	}

	return reached
}

func (d deliveryOracle) required(op Op, h History, reached map[string]struct{}) map[string]struct{} {
	alive := make(map[string]struct{}, len(h.Nodes))
	for _, n := range h.Nodes {
		alive[n] = struct{}{}
	}
	for _, n := range h.Crashed {
		delete(alive, n)
	}

	if d.Liveness == OriginAlive {
		if _, ok := alive[op.Node]; !ok {
			return nil
		}
		return alive
	}

	for r := range reached {
		if _, ok := alive[r]; ok {
			return alive
		}
	}

	return nil
}

// EventualDelivery checks that every accepted op reached the nodes it is owed
// to, judged from the delivery log alone. The harness is CRDT-blind, so this is a
// structural check of the subject's dissemination — did carriers keep flowing
// after the op — not of what they carried. A violation is therefore always
// real: no carrier existed, so the update cannot have arrived. Silence proves
// nothing about content; that is Convergence's and CounterSum's job. Pull
// messages carry no state. One violation per op, naming the origin, the op
// and every node missed.
func EventualDelivery(liveness Liveness, pattern Pattern) Oracle {
	return deliveryOracle{Liveness: liveness, Pattern: pattern}
}

func findFirstLiveNode(obs []Observation, crashed map[string]struct{}) (Observation, bool) {
	var zero Observation
	for _, o := range obs {
		if _, ok := crashed[o.Node]; !ok {
			return o, true
		}
	}
	return zero, false
}
