package simtest

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"
)

// Contract tests for GenScenario: a scenario is a pure function of its seed,
// and everything it draws stays inside the profile's bounds. Hand-written
// scenarios are validated by Run, not here.

func testProfile() Profile {
	return Profile{
		NodesMin:  2,
		NodesMax:  5,
		MaxOps:    20,
		MaxFaults: 6,
		OpGen: func(r *rand.Rand, node int) string {
			return fmt.Sprintf("inc:%d", r.IntN(10)+1)
		},
		FaultKinds: []FaultKind{FaultDrop, FaultDelay, FaultDup, FaultPartition, FaultAckLie},
		Interval:   10,
		Horizon:    200,
		Settle:     100,
	}
}

// Profiles describe the space stress explores, and that space must stay
// inside the delivery contract — a contract-violating network turns stress
// findings into noise. Hand-written scenarios may use FaultBreakAck directly;
// a profile listing it is a programmer error.
func TestGenScenarioRejectsAContractViolatingProfile(t *testing.T) {
	p := testProfile()
	p.FaultKinds = append(p.FaultKinds, FaultBreakAck)
	defer func() {
		if recover() == nil {
			t.Fatal("GenScenario accepted a profile listing FaultBreakAck")
		}
	}()
	GenScenario(1, p)
}

func TestGenScenarioUsesTheTopologyHook(t *testing.T) {
	p := testProfile()
	p.TopoGen = func(r *rand.Rand, nodes int) [][2]int {
		line := make([][2]int, 0, nodes-1)
		for i := 0; i < nodes-1; i++ {
			line = append(line, [2]int{i, i + 1}) // connected by construction
		}
		return line
	}
	s := GenScenario(7, p)
	if len(s.Topology) != s.Nodes-1 {
		t.Fatalf("line over %d nodes should carry %d edges, got %v", s.Nodes, s.Nodes-1, s.Topology)
	}
	for _, e := range s.Topology {
		if e[0] < 0 || e[0] >= s.Nodes || e[1] < 0 || e[1] >= s.Nodes || e[0] == e[1] {
			t.Fatalf("invalid edge %v for %d nodes", e, s.Nodes)
		}
	}
}

func TestGenScenarioRejectsADisconnectedTopology(t *testing.T) {
	p := testProfile()
	p.NodesMin, p.NodesMax = 4, 4
	p.TopoGen = func(r *rand.Rand, nodes int) [][2]int {
		return [][2]int{{0, 1}, {2, 3}} // two islands — delivery oracles would false-fail
	}
	defer func() {
		if recover() == nil {
			t.Fatal("GenScenario accepted a disconnected topology")
		}
	}()
	GenScenario(1, p)
}

// A partition group must be a sample of ALL nodes, not a permutation of the
// first len(group) indexes — this pins the sampling range.
func TestGenScenarioPartitionGroupsSampleAllNodes(t *testing.T) {
	p := testProfile()
	p.NodesMin, p.NodesMax = 5, 5
	p.FaultKinds = []FaultKind{FaultPartition}
	seen := false
	for seed := range uint64(100) {
		for _, f := range GenScenario(seed, p).Faults {
			for _, n := range f.Group {
				if n >= len(f.Group) { // unreachable under prefix-only sampling
					seen = true
				}
			}
		}
	}
	if !seen {
		t.Fatal("across 100 seeds every partition group was a permutation of {0..len-1}: " +
			"the sampler never reaches higher node indexes")
	}
}

// graphIsConnected is a general utility: hand-written showcase topologies may
// be any size. This connected 8-node graph drives the union-find tree deep
// enough that a single path-halving step no longer reaches the root — found
// by adversarial search against a BFS reference.
func TestGraphIsConnectedOnADeepUnionFindTree(t *testing.T) {
	g := [][2]int{{3, 7}, {0, 6}, {1, 5}, {2, 4}, {1, 7}, {0, 2}, {2, 3}, {4, 7}}
	if !graphIsConnected(8, g) {
		t.Fatal("a connected 8-node graph was reported disconnected: " +
			"find must loop path-halving up to the root, a single step is not enough")
	}
}

func TestGenScenarioIsDeterministic(t *testing.T) {
	p := testProfile()
	a := GenScenario(42, p)
	b := GenScenario(42, p)
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("same seed produced different scenarios:\n%+v\nvs\n%+v", a, b)
	}
}

func TestGenScenarioVariesWithSeed(t *testing.T) {
	p := testProfile()
	a := GenScenario(42, p)
	b := GenScenario(43, p)
	if reflect.DeepEqual(a, b) {
		t.Fatal("different seeds produced identical scenarios")
	}
}

func TestGenScenarioStaysWithinTheProfile(t *testing.T) {
	p := testProfile()
	for seed := range uint64(50) {
		s := GenScenario(seed, p)

		if s.Seed != seed {
			t.Fatalf("seed %d: scenario records Seed=%d", seed, s.Seed)
		}
		if s.Nodes < p.NodesMin || s.Nodes > p.NodesMax {
			t.Fatalf("seed %d: Nodes=%d outside [%d,%d]", seed, s.Nodes, p.NodesMin, p.NodesMax)
		}
		if s.Interval != p.Interval || s.Horizon != p.Horizon || s.Settle != p.Settle {
			t.Fatalf("seed %d: timing fields diverge from the profile: %+v", seed, s)
		}
		if s.Topology != nil {
			t.Fatalf("seed %d: no TopoGen in the profile, but Topology=%v (nil is the canonical full mesh)",
				seed, s.Topology)
		}

		if len(s.Ops) > p.MaxOps {
			t.Fatalf("seed %d: %d ops > MaxOps %d", seed, len(s.Ops), p.MaxOps)
		}
		for i, op := range s.Ops {
			if op.At < 0 || op.At > s.Horizon {
				t.Fatalf("seed %d: op %d at %d outside the active phase [0,%d]", seed, i, op.At, s.Horizon)
			}
			if op.Node < 0 || op.Node >= s.Nodes {
				t.Fatalf("seed %d: op %d targets node %d of %d", seed, i, op.Node, s.Nodes)
			}
			if op.Op == "" {
				t.Fatalf("seed %d: op %d is empty", seed, i)
			}
		}

		if len(s.Faults) > p.MaxFaults {
			t.Fatalf("seed %d: %d faults > MaxFaults %d", seed, len(s.Faults), p.MaxFaults)
		}
		for i, f := range s.Faults {
			if f.Kind == FaultBreakAck {
				t.Fatalf("seed %d: generated the contract-violating FaultBreakAck", seed)
			}
			if f.At < 0 || f.Until <= f.At || f.Until > s.Horizon {
				t.Fatalf("seed %d: fault %d window [%d,%d) outside the active phase [0,%d]",
					seed, i, f.At, f.Until, s.Horizon)
			}
			switch f.Kind {
			case FaultDrop, FaultDup, FaultAckLie:
				if f.P <= 0 || f.P > 1 {
					t.Fatalf("seed %d: fault %d (%s) has P=%v outside (0,1]", seed, i, f.Kind, f.P)
				}
			case FaultDelay:
				if f.MinD < 1 || f.MaxD < f.MinD {
					t.Fatalf("seed %d: fault %d delay bounds [%d,%d] invalid", seed, i, f.MinD, f.MaxD)
				}
			case FaultPartition:
				if len(f.Group) == 0 || len(f.Group) >= s.Nodes {
					t.Fatalf("seed %d: fault %d partition group of %d nodes is not a proper subset of %d",
						seed, i, len(f.Group), s.Nodes)
				}
				for _, n := range f.Group {
					if n < 0 || n >= s.Nodes {
						t.Fatalf("seed %d: fault %d partition names node %d of %d", seed, i, n, s.Nodes)
					}
				}
			}
		}
	}
}
