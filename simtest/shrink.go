package simtest

import "slices"

// Shrink reduces a failing scenario to a smaller one that fails the same
// oracles. It removes ops, faults and nodes and lowers the horizon. Seed,
// Interval and Settle stay as they are, and a connected topology stays
// connected. The result is deterministic and valid for Run; it is minimal
// only in that no single further cut keeps the failure. A scenario that does
// not fail is returned unchanged, with its result.
func Shrink(s Scenario, sub Subject, oracles ...Oracle) (Scenario, Result) {
	sh := &shrinker{sub: sub, oracles: oracles, memo: map[string]Result{}}
	res := sh.run(s)
	sh.want = fired(res.Violations)
	if len(sh.want) == 0 {
		return s, res
	}
	sh.connected = graphIsConnected(s.Nodes, s.Topology)

	for changed := true; changed; {
		changed = sh.cut(&s, faultCount, dropFault)
		changed = sh.cut(&s, nodeCount, dropNode) || changed
		changed = sh.cut(&s, opCount, dropOp) || changed
		changed = sh.lowerHorizon(&s) || changed
	}
	return s, sh.run(s)
}

// shrinker memoizes runs by scenario digest: passes revisit candidates.
type shrinker struct {
	sub       Subject
	oracles   []Oracle
	want      []string // the oracles the original fails, sorted
	connected bool     // the original is connected, so every cut must be
	memo      map[string]Result
}

func (sh *shrinker) run(s Scenario) Result {
	key := scenarioDigest(s)
	if r, ok := sh.memo[key]; ok {
		return r
	}
	r := Run(s, sh.sub, sh.oracles...)
	sh.memo[key] = r
	return r
}

func (sh *shrinker) keeps(c Scenario) bool {
	if sh.connected && !graphIsConnected(c.Nodes, c.Topology) {
		return false
	}
	return slices.Equal(fired(sh.run(c).Violations), sh.want)
}

// After an accepted cut the next element sits at the same index.
func (sh *shrinker) cut(s *Scenario, count func(Scenario) int, drop func(Scenario, int) Scenario) bool {
	changed := false
	for i := 0; i < count(*s); {
		if c := drop(*s, i); sh.keeps(c) {
			*s, changed = c, true
		} else {
			i++
		}
	}
	return changed
}

// lowerHorizon tries the last op's instant first, then bisects above it.
func (sh *shrinker) lowerHorizon(s *Scenario) bool {
	lo := Dur(0)
	for _, op := range s.Ops {
		lo = max(lo, op.At)
	}
	if s.Horizon <= lo {
		return false
	}
	if c := withHorizon(*s, lo); sh.keeps(c) {
		*s = c
		return true
	}
	changed := false
	for bad, good := lo, s.Horizon; good-bad > 1; {
		mid := bad + (good-bad)/2
		if c := withHorizon(*s, mid); sh.keeps(c) {
			*s, good, changed = c, mid, true
		} else {
			bad = mid
		}
	}
	return changed
}

func faultCount(s Scenario) int { return len(s.Faults) }

func opCount(s Scenario) int { return len(s.Ops) }

// A run needs a node: the last one is never a candidate.
func nodeCount(s Scenario) int {
	if s.Nodes <= 1 {
		return 0
	}
	return s.Nodes
}

func dropFault(s Scenario, i int) Scenario {
	s.Faults = slices.Delete(slices.Clone(s.Faults), i, i+1)
	return s
}

func dropOp(s Scenario, i int) Scenario {
	s.Ops = slices.Delete(slices.Clone(s.Ops), i, i+1)
	return s
}

// dropNode also renumbers the nodes above j.
func dropNode(s Scenario, j int) Scenario {
	s.Nodes--

	ops := make([]OpEntry, 0, len(s.Ops))
	for _, op := range s.Ops {
		if op.Node == j {
			continue
		}
		if op.Node > j {
			op.Node--
		}
		ops = append(ops, op)
	}
	s.Ops = ops

	topo := make([][2]int, 0, len(s.Topology))
	for _, e := range s.Topology {
		if e[0] == j || e[1] == j {
			continue
		}
		if e[0] > j {
			e[0]--
		}
		if e[1] > j {
			e[1]--
		}
		topo = append(topo, e)
	}
	s.Topology = topo

	faults := make([]FaultEntry, len(s.Faults))
	for k, f := range s.Faults {
		if f.Group != nil {
			g := make([]int, 0, len(f.Group))
			for _, n := range f.Group {
				if n == j {
					continue
				}
				if n > j {
					n--
				}
				g = append(g, n)
			}
			f.Group = g
		}
		faults[k] = f
	}
	s.Faults = faults
	return s
}

// withHorizon keeps fault windows inside the active phase: a lower horizon
// must never leak a fault into settle.
func withHorizon(s Scenario, h Dur) Scenario {
	s.Horizon = h
	faults := make([]FaultEntry, 0, len(s.Faults))
	for _, f := range s.Faults {
		if f.At >= h {
			continue
		}
		f.Until = min(f.Until, h)
		faults = append(faults, f)
	}
	s.Faults = faults
	return s
}

func fired(vs []Violation) []string {
	var names []string
	for _, v := range vs {
		if !slices.Contains(names, v.Oracle) {
			names = append(names, v.Oracle)
		}
	}
	slices.Sort(names)
	return names
}
