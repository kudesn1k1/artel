package causal

import "sort"

type Dot struct {
	Replica string
	N       uint64
}

type VersionVector map[string]uint64

func (v VersionVector) Contains(d Dot) bool {
	return d.N != 0 && v[d.Replica] >= d.N
}

func (v VersionVector) Next(replica string) Dot {
	return Dot{
		Replica: replica,
		N:       v[replica] + 1,
	}
}

func (v VersionVector) Join(other VersionVector) VersionVector {
	res := make(VersionVector, len(v)+len(other))
	for r, n := range v {
		if n != 0 {
			res[r] = n
		}
	}
	for r, n := range other {
		if n != 0 {
			res[r] = max(res[r], n)
		}
	}
	return res
}

type DotSet map[Dot]struct{}

func (ds DotSet) Contains(d Dot) bool {
	_, ok := ds[d]
	return ok
}

type DotMap[T comparable] map[T]DotSet

type CausalContext struct {
	VV   VersionVector
	Dots DotSet
}

func (c CausalContext) Contains(dot Dot) bool {
	return c.VV.Contains(dot) || c.Dots.Contains(dot)
}

func (c CausalContext) Next(replica string) Dot {
	mx := uint64(0)
	for dot := range c.Dots {
		if dot.Replica == replica {
			mx = max(mx, dot.N)
		}
	}
	return Dot{
		Replica: replica,
		N:       max(mx, c.VV[replica]) + 1,
	}
}

func (c CausalContext) Join(other CausalContext) CausalContext {
	res := CausalContext{
		VV:   c.VV.Join(other.VV),
		Dots: make(DotSet),
	}

	dots := make([]Dot, 0, len(c.Dots)+len(other.Dots))
	for dot := range c.Dots {
		dots = append(dots, dot)
	}
	for dot := range other.Dots {
		dots = append(dots, dot)
	}

	sort.Slice(dots, func(i, j int) bool {
		if dots[i].Replica == dots[j].Replica {
			return dots[i].N < dots[j].N
		}
		return dots[i].Replica < dots[j].Replica
	})

	for _, dot := range dots {
		if res.VV.Contains(dot) {
			continue
		}
		if res.VV[dot.Replica]+1 == dot.N {
			res.VV[dot.Replica] = dot.N
			continue
		}
		res.Dots[dot] = struct{}{}
	}

	return res
}

func (c CausalContext) Compact() bool {
	return len(c.Dots) == 0
}

func Join[T comparable](aStore DotMap[T], aCC CausalContext, bStore DotMap[T], bCC CausalContext) (DotMap[T], CausalContext) {
	store := make(DotMap[T], len(aStore)+len(bStore))
	keep := func(val T, dot Dot) {
		if _, exists := store[val]; !exists {
			store[val] = make(DotSet)
		}
		store[val][dot] = struct{}{}
	}

	for val, dots := range bStore {
		for dot := range dots {
			if _, ok := aStore[val][dot]; ok {
				keep(val, dot)
				continue
			}
			if !aCC.Contains(dot) {
				keep(val, dot)
			}
		}
	}

	for val, dots := range aStore {
		for dot := range dots {
			if _, ok := bStore[val][dot]; ok {
				keep(val, dot)
				continue
			}
			if !bCC.Contains(dot) {
				keep(val, dot)
			}
		}
	}

	return store, aCC.Join(bCC)
}
