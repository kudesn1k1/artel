package causal_test

import (
	"reflect"
	"testing"

	"pgregory.net/rapid"

	"github.com/kudesn1k1/artel/internal/causal"
)

// Property tests: the causal join is a join-semilattice operation on every
// (store, context) pair whose store the context covers — states at rest and
// deltas alike — and the context join is exactly set union. Every generator
// draws in a fixed order so a failing seed replays the same values.

var (
	replicas = []string{"a", "b", "c"}
	elements = []string{"x", "y", "z"}
)

// genContext draws a context as a dot set with gaps, spelled canonically.
func genContext() *rapid.Generator[causal.CausalContext] {
	return rapid.Custom(func(t *rapid.T) causal.CausalContext {
		var ds []causal.Dot
		for _, r := range replicas {
			for n := uint64(1); n <= 5; n++ {
				if rapid.Bool().Draw(t, r) {
					ds = append(ds, dot(r, n))
				}
			}
		}
		return canonical(ds)
	})
}

// genState draws a consistent state: every dot the context contains is
// either held by exactly one element or dead (seen, not held) — the shape a
// replica or a delta reaches by adds and removes.
func genState() *rapid.Generator[state] {
	return rapid.Custom(func(t *rapid.T) state {
		cc := genContext().Draw(t, "cc")
		store := causal.DotMap[string]{}
		for _, d := range contextDots(cc) {
			if i := rapid.IntRange(-1, len(elements)-1).Draw(t, "owner"); i >= 0 {
				e := elements[i]
				if store[e] == nil {
					store[e] = causal.DotSet{}
				}
				store[e][d] = struct{}{}
			}
		}
		return state{store, cc}
	})
}

func genDot() *rapid.Generator[causal.Dot] {
	return rapid.Custom(func(t *rapid.T) causal.Dot {
		return dot(rapid.SampledFrom(replicas).Draw(t, "replica"), rapid.Uint64Range(1, 6).Draw(t, "n"))
	})
}

func clone(x state) state { return normalize(x.store, x.cc) }

func TestJoinLaws(t *testing.T) {
	t.Run("commutative", rapid.MakeCheck(func(t *rapid.T) {
		x, y := genState().Draw(t, "x"), genState().Draw(t, "y")
		requireState(t, "x⊔y = y⊔x", join(x, y), join(y, x))
	}))
	t.Run("associative", rapid.MakeCheck(func(t *rapid.T) {
		x, y, z := genState().Draw(t, "x"), genState().Draw(t, "y"), genState().Draw(t, "z")
		requireState(t, "(x⊔y)⊔z = x⊔(y⊔z)", join(join(x, y), z), join(x, join(y, z)))
	}))
	t.Run("idempotent", rapid.MakeCheck(func(t *rapid.T) {
		x, y := genState().Draw(t, "x"), genState().Draw(t, "y")
		requireState(t, "x⊔x = x", join(x, x), x)
		xy := join(x, y)
		requireState(t, "(x⊔y)⊔y = x⊔y", join(xy, y), xy)
	}))
	// The rule itself, checked dot by dot against the definition: a dot
	// survives iff both sides hold it, or one holds it and the other has not
	// seen it.
	t.Run("survival rule", rapid.MakeCheck(func(t *rapid.T) {
		x, y := genState().Draw(t, "x"), genState().Draw(t, "y")
		got := join(x, y)
		for _, e := range elements {
			for _, d := range union(x.store[e], y.store[e]) {
				_, inX := x.store[e][d]
				_, inY := y.store[e][d]
				want := (inX && inY) || (inX && !y.cc.Contains(d)) || (inY && !x.cc.Contains(d))
				if _, ok := got.store[e][d]; ok != want {
					t.Fatalf("%s %v: survives=%v, want %v\n x: %v %v\n y: %v %v", e, d, ok, want, x.store, x.cc, y.store, y.cc)
				}
			}
		}
	}))
	// The context of the result is exactly the union of the two: no dot is
	// lost and none is invented.
	t.Run("context is the union", rapid.MakeCheck(func(t *rapid.T) {
		x, y := genState().Draw(t, "x"), genState().Draw(t, "y")
		got := join(x, y)
		want := canonical(append(contextDots(x.cc), contextDots(y.cc)...))
		if !reflect.DeepEqual(got.cc, want) {
			t.Fatalf("context of the join is %v, want the union %v", got.cc, want)
		}
	}))
	t.Run("result is consistent", rapid.MakeCheck(func(t *rapid.T) {
		x, y := genState().Draw(t, "x"), genState().Draw(t, "y")
		got := join(x, y)
		for e, s := range got.store {
			for d := range s {
				if !got.cc.Contains(d) {
					t.Fatalf("%s holds %v outside the context %v", e, d, got.cc)
				}
			}
		}
	}))
	t.Run("canonical", rapid.MakeCheck(func(t *rapid.T) {
		x, y := genState().Draw(t, "x"), genState().Draw(t, "y")
		store, cc := causal.Join(x.store, x.cc, y.store, y.cc)
		for e, s := range store {
			if len(s) == 0 {
				t.Fatalf("element %s kept with no dots: %v", e, store)
			}
		}
		requireCanonical(t, "join result", cc)
	}))
	t.Run("pure", rapid.MakeCheck(func(t *rapid.T) {
		x, y := genState().Draw(t, "x"), genState().Draw(t, "y")
		xBefore, yBefore := clone(x), clone(y)
		store, cc := causal.Join(x.store, x.cc, y.store, y.cc)
		for _, s := range store {
			s[dot("q", 1)] = struct{}{}
		}
		if cc.VV != nil {
			cc.VV["q"] = 1
		}
		if cc.Dots != nil {
			cc.Dots[dot("q", 3)] = struct{}{}
		}
		requireState(t, "x after join and mutation of the result", clone(x), xBefore)
		requireState(t, "y after join and mutation of the result", clone(y), yBefore)
	}))
}

func TestCausalContextLaws(t *testing.T) {
	t.Run("commutative", rapid.MakeCheck(func(t *rapid.T) {
		a, b := genContext().Draw(t, "a"), genContext().Draw(t, "b")
		requireContext(t, "a⊔b = b⊔a", a.Join(b), b.Join(a))
	}))
	t.Run("associative", rapid.MakeCheck(func(t *rapid.T) {
		a, b, c := genContext().Draw(t, "a"), genContext().Draw(t, "b"), genContext().Draw(t, "c")
		requireContext(t, "(a⊔b)⊔c = a⊔(b⊔c)", a.Join(b).Join(c), a.Join(b.Join(c)))
	}))
	t.Run("idempotent", rapid.MakeCheck(func(t *rapid.T) {
		a := genContext().Draw(t, "a")
		requireContext(t, "a⊔a = a", a.Join(a), a)
	}))
	t.Run("join is set union", rapid.MakeCheck(func(t *rapid.T) {
		a, b := genContext().Draw(t, "a"), genContext().Draw(t, "b")
		got := a.Join(b)
		want := canonical(append(contextDots(a), contextDots(b)...))
		if !reflect.DeepEqual(canonical(contextDots(got)), want) {
			t.Fatalf("%v ⊔ %v = %v, want the union %v", a, b, got, want)
		}
	}))
	t.Run("canonical", rapid.MakeCheck(func(t *rapid.T) {
		a, b := genContext().Draw(t, "a"), genContext().Draw(t, "b")
		requireCanonical(t, "join result", a.Join(b))
	}))
	t.Run("contains agrees with the dot set", rapid.MakeCheck(func(t *rapid.T) {
		a, d := genContext().Draw(t, "a"), genDot().Draw(t, "d")
		_, want := dots(contextDots(a)...)[d]
		if got := a.Contains(d); got != want {
			t.Fatalf("%v contains %v: %v, want %v", a, d, got, want)
		}
	}))
	t.Run("next is fresh and pure", rapid.MakeCheck(func(t *rapid.T) {
		a := genContext().Draw(t, "a")
		r := rapid.SampledFrom(replicas).Draw(t, "replica")
		before := canonical(contextDots(a))
		d := a.Next(r)
		if a.Contains(d) {
			t.Fatalf("%v already contains its next dot %v", a, d)
		}
		for _, held := range contextDots(a) {
			if held.Replica == r && held.N >= d.N {
				t.Fatalf("%v holds %v at or past the next dot %v", a, held, d)
			}
		}
		if !reflect.DeepEqual(canonical(contextDots(a)), before) {
			t.Fatalf("Next changed the context: %v from %v", a, before)
		}
		if !a.Join(ctx(nil, d)).Contains(d) {
			t.Fatalf("%v does not contain %v after joining its fragment", a, d)
		}
	}))
}

func TestVersionVectorLaws(t *testing.T) {
	genVV := rapid.Custom(func(t *rapid.T) causal.VersionVector {
		vv := causal.VersionVector{}
		for _, r := range replicas {
			if n := rapid.Uint64Range(0, 4).Draw(t, r); n > 0 {
				vv[r] = n
			}
		}
		return vv
	})
	t.Run("commutative", rapid.MakeCheck(func(t *rapid.T) {
		a, b := genVV.Draw(t, "a"), genVV.Draw(t, "b")
		if x, y := a.Join(b), b.Join(a); !reflect.DeepEqual(x, y) {
			t.Fatalf("%v ⊔ %v: %v vs %v", a, b, x, y)
		}
	}))
	t.Run("associative", rapid.MakeCheck(func(t *rapid.T) {
		a, b, c := genVV.Draw(t, "a"), genVV.Draw(t, "b"), genVV.Draw(t, "c")
		if x, y := a.Join(b).Join(c), a.Join(b.Join(c)); !reflect.DeepEqual(x, y) {
			t.Fatalf("%v ⊔ %v ⊔ %v: %v vs %v", a, b, c, x, y)
		}
	}))
	t.Run("idempotent", rapid.MakeCheck(func(t *rapid.T) {
		a := genVV.Draw(t, "a")
		if x := a.Join(a); !reflect.DeepEqual(x, a) {
			t.Fatalf("%v ⊔ itself = %v", a, x)
		}
	}))
	t.Run("contains agrees with the entry", rapid.MakeCheck(func(t *rapid.T) {
		a, d := genVV.Draw(t, "a"), genDot().Draw(t, "d")
		if got, want := a.Contains(d), d.N <= a[d.Replica]; got != want {
			t.Fatalf("%v contains %v: %v, want %v", a, d, got, want)
		}
	}))
	t.Run("next is fresh and pure", rapid.MakeCheck(func(t *rapid.T) {
		a := genVV.Draw(t, "a")
		r := rapid.SampledFrom(replicas).Draw(t, "replica")
		before := a.Join(nil)
		d := a.Next(r)
		if a.Contains(d) || d.N != a[r]+1 {
			t.Fatalf("next dot of %s in %v is %v", r, a, d)
		}
		if !reflect.DeepEqual(a, before) {
			t.Fatalf("Next changed the vector: %v from %v", a, before)
		}
	}))
}

// union lists the dots of both sets in a fixed order.
func union(a, b causal.DotSet) []causal.Dot {
	seen := map[causal.Dot]bool{}
	var out []causal.Dot
	for _, s := range []causal.DotSet{a, b} {
		for _, r := range replicas {
			for n := uint64(1); n <= 8; n++ {
				d := dot(r, n)
				if _, ok := s[d]; ok && !seen[d] {
					seen[d] = true
					out = append(out, d)
				}
			}
		}
	}
	return out
}
