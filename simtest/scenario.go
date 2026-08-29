package simtest

import (
	"fmt"
	"math/rand/v2"
)

// Dur is a duration or instant in virtual time units. Virtual time has no
// relation to the wall clock: the scheduler jumps from event to event.
type Dur int64

// OpEntry schedules one local mutation on a node. The Op string is
// interpreted by the Subject (e.g. "inc:5" for a counter, "add:x" for a set),
// which keeps Scenario plain serializable data.
type OpEntry struct {
	At   Dur
	Node int
	Op   string
}

// Scenario is a complete, plain-data description of one simulation run.
// It is a pure function of its seed when generated (GenScenario), a literal
// in hand-written tests, and the unit of minimization for the shrinker —
// the same object serves replay, calibration and the trace header.
type Scenario struct {
	Seed     uint64
	Nodes    int
	Topology [][2]int // nil = full mesh; otherwise explicit undirected edges
	Interval Dur      // gossip tick period; first tick of every node is at t=0
	Ops      []OpEntry
	Faults   []FaultEntry
	Horizon  Dur // end of the active phase (ops and faults live in [0, Horizon])
	Settle   Dur // healing phase after Horizon: faults off, ticks only
}

// Profile bounds the space GenScenario draws scenarios from.
type Profile struct {
	NodesMin, NodesMax int
	MaxOps             int
	MaxFaults          int
	// OpGen produces one op for the given node. It must draw randomness only
	// from r — any other source breaks scenario reproducibility.
	OpGen func(r *rand.Rand, node int) string
	// TopoGen, when set, generates the scenario topology (undirected edges over
	// the drawn node count). It must draw randomness only from r and must return
	// a connected graph — GenScenario panics on a disconnected or invalid one.
	// nil = full mesh (Scenario.Topology stays nil, the canonical full-mesh form).
	TopoGen func(r *rand.Rand, nodes int) [][2]int
	// FaultKinds lists the anomalies the profile may use.
	// FaultAckLie must never be listed.
	FaultKinds []FaultKind
	Interval   Dur
	Horizon    Dur
	Settle     Dur
}

func GenScenario(seed uint64, p Profile) Scenario {
	for _, f := range p.FaultKinds {
		if f == FaultAckLie {
			panic("simtest: FaultBreakAck must not appear in Profile.FaultKinds")
		}
	}

	r := rand.New(rand.NewPCG(seed, 0))
	sc := Scenario{
		Seed:     seed,
		Nodes:    r.IntN(p.NodesMax-p.NodesMin+1) + p.NodesMin,
		Interval: p.Interval,
		Horizon:  p.Horizon,
		Settle:   p.Settle,
	}

	if p.TopoGen != nil {
		sc.Topology = p.TopoGen(r, sc.Nodes)
		if !graphIsConnected(sc.Nodes, sc.Topology) {
			panic(fmt.Sprintf("simtest: TopoGen returned a disconnected graph for %d nodes: %v", sc.Nodes, sc.Topology))
		}
	} else {
		sc.Topology = FullMesh(sc.Nodes)
	}

	sc.Ops = make([]OpEntry, r.IntN(p.MaxOps+1))
	for i := 0; i < len(sc.Ops); i++ {
		node := r.IntN(sc.Nodes)
		sc.Ops[i] = OpEntry{
			At:   Dur(r.Int64N(int64(sc.Horizon) + 1)),
			Node: node,
			Op:   p.OpGen(r, node),
		}
	}

	sc.Faults = make([]FaultEntry, r.IntN(p.MaxFaults+1))
	for i := 0; i < len(sc.Faults); i++ {
		kind := p.FaultKinds[r.IntN(len(p.FaultKinds))]
		at := Dur(r.Int64N(int64(sc.Horizon)))
		until := at + Dur(r.Int64N(int64(sc.Horizon-at))) + 1
		fault := FaultEntry{
			At:    at,
			Until: until,
			Kind:  kind,
		}

		switch kind {
		case FaultDelay:
			fault.MinD = Dur(r.Int64N(int64(sc.Horizon)))
			fault.MaxD = fault.MinD + Dur(r.Int64N(int64(sc.Horizon-fault.MinD)))
		case FaultDrop, FaultDup, FaultAckLost:
			fault.P = r.Float64()
		case FaultPartition:
			fault.Group = genRandomGroup(r, sc.Nodes)
		}

		sc.Faults[i] = fault
	}

	return sc
}

func genRandomGroup(r *rand.Rand, nodes int) []int {
	n := 1 + r.IntN(nodes/2)
	out := make([]int, n)
	swapped := make(map[int]int, n)

	get := func(k int) int {
		if v, ok := swapped[k]; ok {
			return v
		}
		return k
	}

	for i := range n {
		j := i + r.IntN(nodes-i)
		out[i] = get(j)
		swapped[j] = get(i)
		delete(swapped, i)
	}

	return out
}

func FullMesh(nodes int) [][2]int {
	ans := make([][2]int, 0, nodes*(nodes-1)/2)
	for i := range nodes {
		for j := i + 1; j < nodes; j++ {
			ans = append(ans, [2]int{i, j})
		}
	}
	return ans
}

func graphIsConnected(nodes int, g [][2]int) bool {
	if nodes <= 1 {
		return true
	}

	parent := make([]int, nodes)
	rank := make([]int, nodes)
	for i := range parent {
		parent[i] = i
	}

	find := func(x int) int {
		for parent[x] != x {
			parent[x] = parent[parent[x]] // use path-halving to avoid recursion
			x = parent[x]
		}
		return parent[x]
	}

	components := nodes
	for _, e := range g {
		a, b := find(e[0]), find(e[1])
		if a == b {
			continue
		}

		if rank[a] < rank[b] {
			a, b = b, a
		}
		parent[b] = a
		if rank[a] == rank[b] {
			rank[a]++
		}
		components--
	}
	return components == 1
}
