package causal_test

import (
	"reflect"
	"slices"
	"testing"

	"github.com/kudesn1k1/artel/internal/causal"
)

// Hand tables for the causal core.
//
// A causal context is a set of dots in two parts: the vector holds every
// dot 1..vv[r] of each replica r, Dots holds the rest. A replica's context at
// rest is all vector; a delta's context is mostly loose dots — its own run
// starts where the receiver left off, and a removal names the dots it
// observed. Join is exact: it never claims a dot neither side contained, and
// it folds loose dots into the vector as soon as they continue it. The
// causal join keeps a dot iff both sides hold it, or one side holds it and
// the other has not seen it — "seen but not held" is how a removal travels
// without a tombstone.

func dot(r string, n uint64) causal.Dot { return causal.Dot{Replica: r, N: n} }

func dots(ds ...causal.Dot) causal.DotSet {
	s := make(causal.DotSet, len(ds))
	for _, d := range ds {
		s[d] = struct{}{}
	}
	return s
}

func ctx(vv causal.VersionVector, loose ...causal.Dot) causal.CausalContext {
	return causal.CausalContext{VV: vv, Dots: dots(loose...)}
}

// contextDots lists every dot a context contains, however it is spelled.
func contextDots(c causal.CausalContext) []causal.Dot {
	seen := map[causal.Dot]bool{}
	var out []causal.Dot
	add := func(d causal.Dot) {
		if d.N > 0 && !seen[d] {
			seen[d] = true
			out = append(out, d)
		}
	}
	for r, n := range c.VV {
		for i := uint64(1); i <= n; i++ {
			add(dot(r, i))
		}
	}
	for d := range c.Dots {
		add(d)
	}
	slices.SortFunc(out, func(a, b causal.Dot) int {
		if a.Replica != b.Replica {
			return compareStrings(a.Replica, b.Replica)
		}
		return int(a.N) - int(b.N)
	})
	return out
}

func compareStrings(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// canonical spells a dot set the one way a context may: each replica's run
// from 1 goes into the vector, everything else stays loose.
func canonical(ds []causal.Dot) causal.CausalContext {
	set := dots(ds...)
	c := causal.CausalContext{VV: causal.VersionVector{}, Dots: causal.DotSet{}}
	for d := range set {
		if c.VV[d.Replica] != 0 {
			continue
		}
		n := uint64(1)
		for {
			if _, ok := set[dot(d.Replica, n)]; !ok {
				break
			}
			n++
		}
		if n > 1 {
			c.VV[d.Replica] = n - 1
		}
	}
	for d := range set {
		if d.N > c.VV[d.Replica] {
			c.Dots[d] = struct{}{}
		}
	}
	return c
}

// state is a normalized (store, context) pair: no element without dots, the
// context in canonical form, never nil — so two states compare with
// reflect.DeepEqual no matter how an implementation spelled them.
type state struct {
	store causal.DotMap[string]
	cc    causal.CausalContext
}

func normalize(store causal.DotMap[string], cc causal.CausalContext) state {
	n := state{store: causal.DotMap[string]{}, cc: canonical(contextDots(cc))}
	for e, s := range store {
		if len(s) == 0 {
			continue
		}
		cp := make(causal.DotSet, len(s))
		for d := range s {
			cp[d] = struct{}{}
		}
		n.store[e] = cp
	}
	return n
}

// failer is what *testing.T and *rapid.T have in common.
type failer interface {
	Fatalf(format string, args ...any)
}

func join(x, y state) state {
	return normalize(causal.Join(x.store, x.cc, y.store, y.cc))
}

func requireState(t failer, what string, got, want state) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s:\n got  store=%v cc=%v\n want store=%v cc=%v", what, got.store, got.cc, want.store, want.cc)
	}
}

func requireContext(t failer, what string, got, want causal.CausalContext) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	if g, w := canonical(contextDots(got)), canonical(contextDots(want)); !reflect.DeepEqual(g, w) {
		t.Fatalf("%s:\n got  %v\n want %v", what, got, want)
	}
}

// requireCanonical fails unless the context is spelled the one way: no zero
// entry, no loose dot the vector already covers, no loose dot that would
// continue the vector.
func requireCanonical(t failer, what string, c causal.CausalContext) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	for r, n := range c.VV {
		if n == 0 {
			t.Fatalf("%s: zero entry %s in %v", what, r, c)
		}
	}
	for d := range c.Dots {
		if d.N <= c.VV[d.Replica] {
			t.Fatalf("%s: loose dot %v is already covered by the vector in %v", what, d, c)
		}
		if d.N == c.VV[d.Replica]+1 {
			t.Fatalf("%s: loose dot %v continues the vector and was not folded in %v", what, d, c)
		}
	}
}

func TestVersionVectorContains(t *testing.T) {
	v := causal.VersionVector{"a": 3}
	for _, d := range []causal.Dot{dot("a", 1), dot("a", 2), dot("a", 3)} {
		if !v.Contains(d) {
			t.Fatalf("%v does not contain %v", v, d)
		}
	}
	for _, d := range []causal.Dot{dot("a", 4), dot("b", 1)} {
		if v.Contains(d) {
			t.Fatalf("%v contains %v", v, d)
		}
	}
	var zero causal.VersionVector
	if zero.Contains(dot("a", 1)) {
		t.Fatal("the zero vector contains a dot")
	}
}

// Next names the dot a replica would mint next and changes nothing: the dot
// enters the vector through Join, carried by the fragment that used it. Dots
// are 1-based, so the zero vector — a valid, empty context — yields 1.
func TestVersionVectorNextIsPure(t *testing.T) {
	var v causal.VersionVector
	d := v.Next("a")
	if d != dot("a", 1) {
		t.Fatalf("first dot is %v, want %v", d, dot("a", 1))
	}
	if v.Contains(d) || len(v) != 0 {
		t.Fatalf("Next advanced the vector: %v", v)
	}
	if again := v.Next("a"); again != d {
		t.Fatalf("Next is not repeatable: %v then %v", d, again)
	}

	v = v.Join(causal.VersionVector{d.Replica: d.N})
	if !v.Contains(d) {
		t.Fatalf("%v does not contain %v after joining its fragment", v, d)
	}
	if next := v.Next("a"); next != dot("a", 2) {
		t.Fatalf("next dot is %v, want %v", next, dot("a", 2))
	}
	if next := v.Next("b"); next != dot("b", 1) {
		t.Fatalf("next dot of an unseen replica is %v, want %v", next, dot("b", 1))
	}
}

func TestVersionVectorJoin(t *testing.T) {
	a := causal.VersionVector{"a": 3, "b": 1}
	b := causal.VersionVector{"b": 4, "c": 2}
	want := causal.VersionVector{"a": 3, "b": 4, "c": 2}
	for _, got := range []causal.VersionVector{a.Join(b), b.Join(a)} {
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("join is %v, want %v", got, want)
		}
	}

	got := a.Join(b)
	got["a"] = 99
	if a["a"] != 3 {
		t.Fatal("the result aliases its receiver")
	}
	if b["b"] != 4 || len(b) != 2 {
		t.Fatalf("the argument changed: %v", b)
	}

	var zero causal.VersionVector
	if !reflect.DeepEqual(zero.Join(a), a) || !reflect.DeepEqual(a.Join(zero), a) {
		t.Fatal("the zero vector is not the identity of Join")
	}
	if got := zero.Join(zero); len(got) != 0 {
		t.Fatalf("zero ⊔ zero = %v, want empty", got)
	}
	if v := (causal.VersionVector{"a": 0}).Join(causal.VersionVector{"b": 0}); len(v) != 0 {
		t.Fatalf("zero entries survived VersionVector.Join: %v", v)
	}
}

// The vector alone is the compact spelling of a run from 1: an entry a:2
// means both a:1 and a:2. That is why a delta never puts its dots into a
// vector — see the context tests — and why a context at rest can be one.
func TestVersionVectorEntryIsARunFromOne(t *testing.T) {
	v := causal.VersionVector{"a": 2}
	if !v.Contains(dot("a", 1)) || !v.Contains(dot("a", 2)) || v.Contains(dot("a", 3)) {
		t.Fatalf("%v is not the run a:1..2", v)
	}
}

func TestCausalContextContains(t *testing.T) {
	c := ctx(causal.VersionVector{"a": 2}, dot("a", 4), dot("b", 3))
	for _, d := range []causal.Dot{dot("a", 1), dot("a", 2), dot("a", 4), dot("b", 3)} {
		if !c.Contains(d) {
			t.Fatalf("%v does not contain %v", c, d)
		}
	}
	for _, d := range []causal.Dot{dot("a", 3), dot("a", 5), dot("b", 1), dot("b", 2), dot("c", 1)} {
		if c.Contains(d) {
			t.Fatalf("%v contains %v", c, d)
		}
	}
	var zero causal.CausalContext
	if zero.Contains(dot("a", 1)) {
		t.Fatal("the zero context contains a dot")
	}
	if !zero.Compact() || (ctx(nil, dot("a", 2))).Compact() {
		t.Fatal("Compact is not \"no loose dots\"")
	}
}

// Next on a context is one past the highest dot of the replica, loose dots
// included, and changes nothing. Counting loose dots is the paper's next_i(c);
// a replica never mints on a context holding loose dots of its own, so the
// rule matters only for consistency, never for a real run.
func TestCausalContextNextIsPure(t *testing.T) {
	var zero causal.CausalContext
	if d := zero.Next("a"); d != dot("a", 1) {
		t.Fatalf("first dot is %v, want a:1", d)
	}
	if !zero.Compact() || zero.Contains(dot("a", 1)) {
		t.Fatalf("Next changed the zero context: %v", zero)
	}
	c := ctx(causal.VersionVector{"a": 3})
	if d := c.Next("a"); d != dot("a", 4) {
		t.Fatalf("next dot is %v, want a:4", d)
	}
	c = ctx(causal.VersionVector{"a": 2}, dot("a", 5))
	if d := c.Next("a"); d != dot("a", 6) {
		t.Fatalf("next dot past a loose a:5 is %v, want a:6", d)
	}
	if d := c.Next("b"); d != dot("b", 1) {
		t.Fatalf("next dot of an unseen replica is %v, want b:1", d)
	}
	if !reflect.DeepEqual(c, ctx(causal.VersionVector{"a": 2}, dot("a", 5))) {
		t.Fatalf("Next changed the context: %v", c)
	}
}

// Every row is checked in both directions and its result must be canonical.
func TestCausalContextJoin(t *testing.T) {
	rows := []struct {
		name    string
		a, b    causal.CausalContext
		want    causal.CausalContext
		compact bool
	}{
		{
			"vectors join pointwise",
			ctx(causal.VersionVector{"a": 3, "b": 1}), ctx(causal.VersionVector{"b": 4, "c": 2}),
			ctx(causal.VersionVector{"a": 3, "b": 4, "c": 2}), true,
		},
		{
			"a dot that does not continue the vector stays loose: nothing below it is claimed",
			ctx(nil), ctx(nil, dot("a", 2)),
			ctx(nil, dot("a", 2)), false,
		},
		{
			"a dot that continues the vector is folded",
			ctx(causal.VersionVector{"a": 1}), ctx(nil, dot("a", 2)),
			ctx(causal.VersionVector{"a": 2}), true,
		},
		{
			"folding continues through loose dots already held",
			ctx(causal.VersionVector{"a": 2}, dot("a", 4), dot("a", 5), dot("a", 7)), ctx(nil, dot("a", 3)),
			ctx(causal.VersionVector{"a": 5}, dot("a", 7)), false,
		},
		{
			"a loose dot the vector already covers vanishes",
			ctx(causal.VersionVector{"a": 3}), ctx(nil, dot("a", 2)),
			ctx(causal.VersionVector{"a": 3}), true,
		},
		{
			"a delta's run starting past the vector is folded whole",
			ctx(causal.VersionVector{"a": 4}), ctx(nil, dot("a", 5), dot("a", 6), dot("a", 7)),
			ctx(causal.VersionVector{"a": 7}), true,
		},
		{
			"nothing with nothing",
			ctx(nil), ctx(nil),
			ctx(nil), true,
		},
	}
	for _, r := range rows {
		for _, dir := range []struct {
			name string
			x, y causal.CausalContext
		}{{"a⊔b", r.a, r.b}, {"b⊔a", r.b, r.a}} {
			got := dir.x.Join(dir.y)
			requireContext(t, r.name+" ("+dir.name+")", got, r.want)
			requireCanonical(t, r.name+" ("+dir.name+")", got)
			if got.Compact() != r.compact {
				t.Fatalf("%s (%s): Compact() = %v, want %v: %v", r.name, dir.name, got.Compact(), r.compact, got)
			}
		}
	}
}

// The context a delta carries is exact: joining {a:2} into nothing does not
// make a:1 seen. A vector could only have said "a:1..2" — the lie that turns
// a later a:1 into a phantom removal.
func TestCausalContextJoinNeverClaimsAnUnseenDot(t *testing.T) {
	c := causal.CausalContext{}.Join(ctx(nil, dot("a", 2)))
	if c.Contains(dot("a", 1)) {
		t.Fatalf("%v claims a:1, which neither side contained", c)
	}
	if !c.Contains(dot("a", 2)) {
		t.Fatalf("%v lost a:2", c)
	}
}

func TestCausalContextJoinIsPure(t *testing.T) {
	a := ctx(causal.VersionVector{"a": 1}, dot("a", 3))
	b := ctx(causal.VersionVector{"b": 1}, dot("a", 2))
	got := a.Join(b)
	requireContext(t, "a⊔b", got, ctx(causal.VersionVector{"a": 3, "b": 1}))

	got.VV["z"] = 9
	got.Dots[dot("z", 5)] = struct{}{}
	if !reflect.DeepEqual(a, ctx(causal.VersionVector{"a": 1}, dot("a", 3))) {
		t.Fatalf("mutating the result changed the receiver: %v", a)
	}
	if !reflect.DeepEqual(b, ctx(causal.VersionVector{"b": 1}, dot("a", 2))) {
		t.Fatalf("mutating the result changed the argument: %v", b)
	}
	got = a.Join(b)
	a.Dots[dot("a", 4)] = struct{}{}
	b.VV["b"] = 7
	requireContext(t, "result after mutating the inputs", got, ctx(causal.VersionVector{"a": 3, "b": 1}))
	if c := (causal.CausalContext{VV: causal.VersionVector{"a": 0}}).Join(causal.CausalContext{}); len(c.VV) != 0 {
		t.Fatalf("zero entry survived: %v", c)
	}
}

// Every row is checked in both directions.
func TestJoin(t *testing.T) {
	a1, a2, b1 := dot("a", 1), dot("a", 2), dot("b", 1)
	rows := []struct {
		name      string
		aStore    causal.DotMap[string]
		aCC       causal.CausalContext
		bStore    causal.DotMap[string]
		bCC       causal.CausalContext
		wantStore causal.DotMap[string]
		wantCC    causal.CausalContext
	}{
		{
			"a dot held by both sides lives",
			causal.DotMap[string]{"x": dots(a1)}, ctx(causal.VersionVector{"a": 1}),
			causal.DotMap[string]{"x": dots(a1)}, ctx(causal.VersionVector{"a": 1}),
			causal.DotMap[string]{"x": dots(a1)}, ctx(causal.VersionVector{"a": 1}),
		},
		{
			"a dot the other side has seen but does not hold is dead (observed remove)",
			causal.DotMap[string]{"x": dots(a1)}, ctx(causal.VersionVector{"a": 1}),
			nil, ctx(causal.VersionVector{"a": 1}),
			nil, ctx(causal.VersionVector{"a": 1}),
		},
		{
			"a dot the other side has not seen lives (concurrent add)",
			causal.DotMap[string]{"x": dots(a1)}, ctx(causal.VersionVector{"a": 1}),
			causal.DotMap[string]{"y": dots(b1)}, ctx(causal.VersionVector{"b": 1}),
			causal.DotMap[string]{"x": dots(a1), "y": dots(b1)}, ctx(causal.VersionVector{"a": 1, "b": 1}),
		},
		{
			"add wins: a re-add the remover has not seen survives with its new dot",
			causal.DotMap[string]{"x": dots(a2)}, ctx(causal.VersionVector{"a": 2}),
			nil, ctx(causal.VersionVector{"a": 1}),
			causal.DotMap[string]{"x": dots(a2)}, ctx(causal.VersionVector{"a": 2}),
		},
		{
			"one element tagged by two replicas, one tag removed on one side",
			causal.DotMap[string]{"x": dots(a1, b1)}, ctx(causal.VersionVector{"a": 1, "b": 1}),
			causal.DotMap[string]{"x": dots(b1)}, ctx(causal.VersionVector{"a": 1, "b": 1}),
			causal.DotMap[string]{"x": dots(b1)}, ctx(causal.VersionVector{"a": 1, "b": 1}),
		},
		{
			"an add fragment: its loose dot folds and touches nothing else",
			causal.DotMap[string]{"x": dots(a1)}, ctx(causal.VersionVector{"a": 1}),
			causal.DotMap[string]{"y": dots(a2)}, ctx(nil, a2),
			causal.DotMap[string]{"x": dots(a1), "y": dots(a2)}, ctx(causal.VersionVector{"a": 2}),
		},
		{
			"a remove fragment: the dot it names dies, the rest lives",
			causal.DotMap[string]{"x": dots(a1), "y": dots(a2)}, ctx(causal.VersionVector{"a": 2}),
			nil, ctx(nil, a2),
			causal.DotMap[string]{"x": dots(a1)}, ctx(causal.VersionVector{"a": 2}),
		},
		{
			"a remove fragment naming a dot the receiver has not seen: nothing is claimed below it",
			nil, ctx(nil),
			nil, ctx(nil, a2),
			nil, ctx(nil, a2),
		},
		{
			"contexts merge while the stores stay empty",
			nil, ctx(causal.VersionVector{"a": 3}),
			nil, ctx(causal.VersionVector{"a": 1, "b": 2}),
			nil, ctx(causal.VersionVector{"a": 3, "b": 2}),
		},
		{
			"nothing joined with nothing is nothing",
			nil, ctx(nil),
			nil, ctx(nil),
			nil, ctx(nil),
		},
	}
	for _, r := range rows {
		a, b := state{r.aStore, r.aCC}, state{r.bStore, r.bCC}
		want := normalize(r.wantStore, r.wantCC)
		requireState(t, r.name+" (a⊔b)", join(a, b), want)
		requireState(t, r.name+" (b⊔a)", join(b, a), want)
	}
}

// The cross-replica race, step by step. a adds f (a:1) and e (a:2); b learns
// both and removes e; c receives b's removal before a's adds. An exact
// context keeps a:1 unseen at c, so f survives when a's adds arrive and only
// e dies. (A vector in c's place would have said a:1..2 and killed f too.)
func TestJoinKeepsAnUnseenAddAliveAcrossReplicas(t *testing.T) {
	a1, a2 := dot("a", 1), dot("a", 2)
	addsFromA := state{causal.DotMap[string]{"f": dots(a1), "e": dots(a2)}, ctx(nil, a1, a2)}
	removeFromB := state{nil, ctx(nil, a2)}

	c := join(state{nil, ctx(nil)}, removeFromB)
	requireState(t, "c after b's removal", c, normalize(nil, ctx(nil, a2)))
	if c.cc.Contains(a1) {
		t.Fatalf("c claims a:1 before ever seeing it: %v", c.cc)
	}

	c = join(c, addsFromA)
	requireState(t, "c after a's adds", c, normalize(causal.DotMap[string]{"f": dots(a1)}, ctx(causal.VersionVector{"a": 2})))
}

func TestJoinIsIdempotent(t *testing.T) {
	x := state{
		causal.DotMap[string]{"x": dots(dot("a", 1), dot("b", 2)), "y": dots(dot("b", 1))},
		ctx(causal.VersionVector{"a": 2, "b": 2}, dot("c", 3)),
	}
	requireState(t, "x⊔x", join(x, x), normalize(x.store, x.cc))
	y := state{causal.DotMap[string]{"z": dots(dot("c", 1))}, ctx(causal.VersionVector{"a": 1, "c": 1})}
	xy := join(x, y)
	requireState(t, "(x⊔y)⊔y", join(xy, y), xy)
	requireState(t, "(x⊔y)⊔x", join(xy, x), xy)
}

// Join builds fresh maps: the result shares nothing with its inputs, and the
// inputs come out as they went in.
func TestJoinIsPure(t *testing.T) {
	aStore := causal.DotMap[string]{"x": dots(dot("a", 1))}
	aCC := ctx(causal.VersionVector{"a": 1})
	bStore := causal.DotMap[string]{"x": dots(dot("a", 1), dot("b", 1))}
	bCC := ctx(causal.VersionVector{"a": 1}, dot("b", 1))
	a, b := normalize(aStore, aCC), normalize(bStore, bCC)

	gotStore, gotCC := causal.Join(aStore, aCC, bStore, bCC)
	requireState(t, "input a after the join", normalize(aStore, aCC), a)
	requireState(t, "input b after the join", normalize(bStore, bCC), b)

	gotStore["x"][dot("c", 9)] = struct{}{}
	gotCC.VV["c"] = 9
	gotCC.Dots[dot("c", 11)] = struct{}{}
	requireState(t, "input a after mutating the result", normalize(aStore, aCC), a)
	requireState(t, "input b after mutating the result", normalize(bStore, bCC), b)

	gotStore, gotCC = causal.Join(aStore, aCC, bStore, bCC)
	want := normalize(gotStore, gotCC)
	aStore["x"][dot("a", 7)] = struct{}{}
	bCC.VV["a"] = 7
	bCC.Dots[dot("b", 4)] = struct{}{}
	requireState(t, "result after mutating the inputs", normalize(gotStore, gotCC), want)
}

// The result is canonical: an element whose last dot died is dropped rather
// than kept with an empty set, and the context is spelled the one way. Equal
// states must marshal to equal bytes, so canonical form is made here, not in
// the codec.
func TestJoinResultIsCanonical(t *testing.T) {
	store, cc := causal.Join(causal.DotMap[string]{"x": dots(dot("a", 1))}, ctx(causal.VersionVector{"a": 1}), nil, ctx(causal.VersionVector{"a": 1}))
	if s, ok := store["x"]; ok {
		t.Fatalf("dead element x kept in the store with %v", s)
	}
	requireCanonical(t, "after an observed remove", cc)

	_, cc = causal.Join(causal.DotMap[string](nil), ctx(causal.VersionVector{"a": 0}), nil, ctx(nil, dot("b", 1), dot("b", 2), dot("b", 4)))
	requireCanonical(t, "zero entry and a foldable run", cc)
	requireContext(t, "zero entry and a foldable run", cc, ctx(causal.VersionVector{"b": 2}, dot("b", 4)))
}
