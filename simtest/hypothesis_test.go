package simtest

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kudesn1k1/artel"
	"github.com/kudesn1k1/artel/internal/causal"
	"github.com/kudesn1k1/artel/transport"
)

// The delta OR-Set on live engines under chaos, restarts included. The
// question this run answers: how often does the direct-mesh engine hand the
// set a delta it must refuse — one whose dots do not continue what the set
// has seen from their origin — and does the cluster still converge after
// that? The simulator answers it for the protocol on a hand-computed
// scenario; this run answers it for the real engine over many random
// scenarios, and it is the run a protocol with causal delivery has to pass
// with zero refusals. Each scenario is a function of its seed: the nodes,
// their chaos, the ops and the optional restart. A refused delta is the
// measurement, not a failure: the test asserts only what must hold
// regardless, logs the counts and the refused seeds, and ARTEL_HYPOTHESIS_N
// sets how many scenarios run (the default fits CI).
//
// What must hold: a scenario with no refusal converges once the network is
// clean, and its final sets match the op log. The log is read soundly: an
// element whose last op is an add is present everywhere, since a remove
// cannot undo an add it never observed; an element never added is absent;
// an element whose last op is a remove is unconstrained, because that remove
// may have missed a concurrent add. Ops on a node that restarts are drawn
// only after its restart, so nothing in the log was lost with a dead
// incarnation.

const (
	hypothesisDefaultN = 50
	hypothesisHorizon  = 60 * time.Millisecond
	hypothesisSettle   = 500 * time.Millisecond
)

var hypothesisElements = []string{"a", "b"}

type liveOp struct {
	at   time.Duration
	node int
	op   string
}

type liveRestart struct {
	at   time.Duration
	node int
}

type liveScenario struct {
	seed    uint64
	nodes   int
	chaos   []ChaosConfig
	ops     []liveOp
	restart *liveRestart
}

func genLiveScenario(seed uint64) liveScenario {
	r := rand.New(rand.NewPCG(seed, 0))
	s := liveScenario{seed: seed, nodes: 2 + r.IntN(3)}
	for range s.nodes {
		s.chaos = append(s.chaos, ChaosConfig{
			DropP: r.Float64() * 0.4, DupP: r.Float64() * 0.4, AckLostP: r.Float64() * 0.4,
			DelayMax: time.Duration(r.IntN(4)) * time.Millisecond,
		})
	}
	if r.IntN(2) == 0 {
		s.restart = &liveRestart{at: time.Duration(r.Int64N(int64(hypothesisHorizon / 2))), node: r.IntN(s.nodes)}
	}
	for range 6 + r.IntN(15) {
		op := liveOp{at: time.Duration(r.Int64N(int64(hypothesisHorizon))), node: r.IntN(s.nodes)}
		if s.restart != nil && op.node == s.restart.node && op.at < s.restart.at {
			continue
		}
		e := hypothesisElements[r.IntN(len(hypothesisElements))]
		if r.IntN(3) == 0 {
			op.op = "rm:" + e
		} else {
			op.op = "add:" + e
		}
		s.ops = append(s.ops, op)
	}
	sort.SliceStable(s.ops, func(i, j int) bool { return s.ops[i].at < s.ops[j].at })
	return s
}

// expected reads the op log: present is every element whose last op is an
// add, absent every element never added.
func (s liveScenario) expected() (present, absent []string) {
	last := make(map[string]string)
	for _, op := range s.ops {
		kind, e, _ := strings.Cut(op.op, ":")
		last[e] = kind
	}
	for _, e := range hypothesisElements {
		switch last[e] {
		case "add":
			present = append(present, e)
		case "":
			absent = append(absent, e)
		}
	}
	return present, absent
}

// phased routes Send through the chaos transport while the scenario is
// active and through the plain one after it; everything else is the plain
// transport's. A delayed send already in flight at the switch finishes under
// chaos, so the clean phase starts within one delay of the switch.
type phased struct {
	artel.Transport
	cur atomic.Pointer[artel.Transport]
}

func newPhased(plain, chaos artel.Transport) *phased {
	p := &phased{Transport: plain}
	p.cur.Store(&chaos)
	return p
}

func (p *phased) Send(ctx context.Context, peerID string, m artel.Message) error {
	return (*p.cur.Load()).Send(ctx, peerID, m)
}

func (p *phased) settle() {
	plain := p.Transport
	p.cur.Store(&plain)
}

type liveSet struct {
	id     string
	set    *causal.ORSet[string]
	engine *artel.Engine[causal.ORSetState[string], *causal.ORSet[string]]
}

func startSet(t *testing.T, id string, incarnation int, tr artel.Transport, codec artel.Codec[causal.ORSetState[string]]) *liveSet {
	t.Helper()
	set := causal.NewORSet[string](replicaID(id, incarnation))
	e := artel.NewEngine(set, tr, codec)
	t.Cleanup(func() { _ = e.Stop(context.Background()) })
	if err := e.Start(context.Background(), liveTick); err != nil {
		t.Fatalf("start %s: %v", id, err)
	}
	return &liveSet{id: id, set: set, engine: e}
}

func (n *liveSet) observe() Observation {
	state, err := causal.ORSetJSON[string]().Encode(n.set.State())
	if err != nil {
		panic(err)
	}
	return Observation{Node: n.id, State: state, Value: setValue(n.set.Elements())}
}

type liveResult struct {
	refusals  int
	converged bool
	final     []Observation
}

// runLiveScenario drives one scenario: the engines start behind their chaos
// transports, the ops and the restart happen at their offsets, the wires go
// clean at the horizon, and the run waits for the live replicas to converge
// or for the settle window to pass. Refusals are counted over every
// incarnation, dead ones included.
func runLiveScenario(t *testing.T, s liveScenario, codec artel.Codec[causal.ORSetState[string]]) liveResult {
	t.Helper()
	reg := transport.NewInProcessRegistry()
	ids := make([]string, s.nodes)
	for i := range ids {
		ids[i] = nodeID(i)
	}
	wires := make([]*phased, s.nodes)
	live := make([]*liveSet, s.nodes)
	var all []*causal.ORSet[string]
	for i, id := range ids {
		plain := transport.NewInProcess(id, peersOf(ids, id), reg)
		wires[i] = newPhased(plain, Chaos(plain, s.seed*uint64(s.nodes)+uint64(i), s.chaos[i]))
		live[i] = startSet(t, id, 1, wires[i], codec)
		all = append(all, live[i].set)
	}

	start := time.Now()
	restartDone := s.restart == nil
	for _, op := range s.ops {
		if !restartDone && s.restart.at <= op.at {
			time.Sleep(time.Until(start.Add(s.restart.at)))
			k := s.restart.node
			if err := live[k].engine.Stop(context.Background()); err != nil {
				t.Fatalf("stop %s: %v", ids[k], err)
			}
			live[k] = startSet(t, ids[k], 2, wires[k], codec)
			all = append(all, live[k].set)
			restartDone = true
		}
		time.Sleep(time.Until(start.Add(op.at)))
		kind, e, _ := strings.Cut(op.op, ":")
		if kind == "add" {
			live[op.node].set.Add(e)
		} else {
			live[op.node].set.Remove(e)
		}
	}
	time.Sleep(time.Until(start.Add(hypothesisHorizon)))
	for _, w := range wires {
		w.settle()
	}

	observe := func() []Observation {
		final := make([]Observation, 0, len(live))
		for _, n := range live {
			final = append(final, n.observe())
		}
		return final
	}
	h := History{Nodes: ids}
	res := liveResult{}
	deadline := time.Now().Add(hypothesisSettle)
	for {
		res.final = observe()
		if res.converged = len(Convergence().Check(h, res.final)) == 0; res.converged || time.Now().After(deadline) {
			break
		}
		time.Sleep(liveTick)
	}
	for _, n := range live {
		_ = n.engine.Stop(context.Background())
	}
	for _, set := range all {
		res.refusals += set.Rejected()
	}
	return res
}

func hypothesisN(t *testing.T) int {
	t.Helper()
	v := os.Getenv("ARTEL_HYPOTHESIS_N")
	if v == "" {
		return hypothesisDefaultN
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		t.Fatalf("ARTEL_HYPOTHESIS_N = %q, want a positive integer", v)
	}
	return n
}

// checkTable holds every live set to the op log's sound reading.
func checkTable(t *testing.T, s liveScenario, final []Observation) {
	t.Helper()
	present, absent := s.expected()
	for _, o := range final {
		for _, e := range present {
			if !strings.Contains(o.Value, e) {
				t.Errorf("seed %d: %s reads %s, want %q present (its last op is an add)", s.seed, o.Node, o.Value, e)
			}
		}
		for _, e := range absent {
			if strings.Contains(o.Value, e) {
				t.Errorf("seed %d: %s reads %s, want %q absent (it was never added)", s.seed, o.Node, o.Value, e)
			}
		}
	}
}

type hypothesisTally struct {
	mu                           sync.Mutex
	scenarios, refused, refusals int
	diverged                     int
	elapsed                      time.Duration
}

func (h *hypothesisTally) add(res liveResult, took time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.scenarios++
	h.elapsed += took
	if res.refusals > 0 {
		h.refused++
		h.refusals += res.refusals
	}
	if !res.converged {
		h.diverged++
	}
}

// Every scenario with no refusal must converge and match the op log;
// scenarios with refusals are counted and their seeds printed.
func TestLiveORSetRefusalsUnderChaos(t *testing.T) {
	n := hypothesisN(t)
	tally := &hypothesisTally{}
	var refusedSeeds []uint64
	var mu sync.Mutex
	t.Run("scenarios", func(t *testing.T) {
		for seed := uint64(1); seed <= uint64(n); seed++ {
			t.Run(fmt.Sprint(seed), func(t *testing.T) {
				t.Parallel()
				s := genLiveScenario(seed)
				began := time.Now()
				res := runLiveScenario(t, s, causal.ORSetJSON[string]())
				tally.add(res, time.Since(began))
				if res.refusals > 0 {
					mu.Lock()
					refusedSeeds = append(refusedSeeds, seed)
					mu.Unlock()
					return
				}
				if !res.converged {
					t.Fatalf("seed %d: no refusal, yet the replicas did not converge: %v", seed, values(res.final))
				}
				checkTable(t, s, res.final)
			})
		}
	})
	slices.Sort(refusedSeeds)
	t.Logf("%d scenarios: %d with refusals (%d refusals in total), %d did not converge; %v per scenario; refused seeds %v",
		tally.scenarios, tally.refused, tally.refusals, tally.diverged, tally.elapsed/time.Duration(max(1, tally.scenarios)), refusedSeeds)
}

func values(final []Observation) []string {
	out := make([]string, 0, len(final))
	for _, o := range final {
		out = append(out, o.Node+"="+o.Value)
	}
	return out
}

// The live harness proves it can see a broken type: a set whose deltas carry
// a vector-only context never refuses, and under the same scenarios it
// diverges or contradicts the op log. A run that catches nothing means the
// harness went blind.
func TestLiveHarnessCatchesTheVectorOnlyContext(t *testing.T) {
	const n = 30
	var caught atomic.Int32
	t.Run("scenarios", func(t *testing.T) {
		for seed := uint64(1); seed <= n; seed++ {
			t.Run(fmt.Sprint(seed), func(t *testing.T) {
				t.Parallel()
				s := genLiveScenario(seed)
				res := runLiveScenario(t, s, vectorOnlyJSON())
				if res.refusals > 0 {
					t.Fatalf("seed %d: a vector-only context refused %d deltas, it cannot see a gap", seed, res.refusals)
				}
				if !res.converged || tableBroken(s, res.final) {
					caught.Add(1)
				}
			})
		}
	})
	if caught.Load() == 0 {
		t.Fatalf("no scenario of %d caught the vector-only context: the live harness cannot see it", n)
	}
}

func tableBroken(s liveScenario, final []Observation) bool {
	present, absent := s.expected()
	for _, o := range final {
		for _, e := range present {
			if !strings.Contains(o.Value, e) {
				return true
			}
		}
		for _, e := range absent {
			if strings.Contains(o.Value, e) {
				return true
			}
		}
	}
	return false
}
