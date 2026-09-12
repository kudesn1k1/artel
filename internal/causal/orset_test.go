package causal_test

import (
	"bytes"
	"cmp"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/kudesn1k1/artel/internal/causal"
)

// Hand tables for the delta OR-Set (add-wins) on the causal core. One type
// spells both a state at rest and a delta; a delta's context is exact, so a
// delta whose dots do not continue what the replica holds is refused whole:
// the state stays as it was and Rejected grows by one. Tests read the set
// through sortedElements, since Elements promises no order.

type (
	orset      = causal.ORSet[string]
	orsetState = causal.ORSetState[string]
)

var (
	newSet = causal.NewORSet[string]
	codec  = causal.ORSetJSON[string]()
)

// sortedElements reads the set in a fixed order; the set itself promises
// none.
func sortedElements(r *orset) []string {
	return slices.Sorted(slices.Values(r.Elements()))
}

func requireElements(t failer, what string, r *orset, want ...string) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	got := sortedElements(r)
	if !slices.Equal(got, want) {
		t.Fatalf("%s: elements are %v, want %v", what, got, want)
	}
	for _, e := range want {
		if !r.Contains(e) {
			t.Fatalf("%s: Elements lists %q but Contains denies it", what, e)
		}
	}
}

func requireRejected(t failer, what string, r *orset, want int) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	if got := r.Rejected(); got != want {
		t.Fatalf("%s: Rejected() = %d, want %d", what, got, want)
	}
}

func bytesOf(t failer, s orsetState) []byte {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	b, err := codec.Encode(s)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return b
}

// requireSameBytes is state equality as the simulator sees it: two equal
// states must marshal to the same bytes.
func requireSameBytes(t failer, what string, got, want orsetState) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	if g, w := bytesOf(t, got), bytesOf(t, want); !bytes.Equal(g, w) {
		t.Fatalf("%s:\n got  %s\n want %s", what, g, w)
	}
}

func TestORSetAddRemove(t *testing.T) {
	r := newSet("a")
	requireElements(t, "fresh", r)
	if r.Contains("x") {
		t.Fatal("a fresh set contains x")
	}

	r.Add("y")
	r.Add("x")
	requireElements(t, "after two adds", r, "x", "y")

	r.Remove("x")
	requireElements(t, "after removing x", r, "y")
	if r.Contains("x") {
		t.Fatal("x survived its removal")
	}

	r.Add("x")
	requireElements(t, "after re-adding x", r, "x", "y")
	r.Add("x")
	requireElements(t, "after adding x again", r, "x", "y")

	es := r.Elements()
	es[0] = "mutated"
	requireElements(t, "after mutating a returned slice", r, "x", "y")
}

func TestORSetRemoveOfAnAbsentElementShipsNothing(t *testing.T) {
	r := newSet("a")
	r.Remove("x")
	if !r.Delta().IsBottom() {
		t.Fatalf("removing an absent element produced a delta: %s", bytesOf(t, r.Delta()))
	}
	if !r.State().IsBottom() {
		t.Fatalf("removing an absent element changed the state: %s", bytesOf(t, r.State()))
	}

	r.Add("x")
	r.Remove("x")
	r.FlushDelta()
	r.Remove("x")
	if !r.Delta().IsBottom() {
		t.Fatalf("removing an element removed already produced a delta: %s", bytesOf(t, r.Delta()))
	}
}

// The zero value is bottom, joins as the identity and merges as a no-op:
// the engine starts every buffer from it.
func TestORSetBottom(t *testing.T) {
	var zero orsetState
	if !zero.IsBottom() {
		t.Fatal("the zero value is not bottom")
	}
	if !zero.Join(zero).IsBottom() {
		t.Fatal("bottom ⊔ bottom is not bottom")
	}

	r := newSet("a")
	if !r.State().IsBottom() || !r.Delta().IsBottom() {
		t.Fatal("a fresh replica is not at bottom")
	}

	r.Add("x")
	s := r.State()
	if s.IsBottom() {
		t.Fatal("a state holding x reads as bottom")
	}
	requireSameBytes(t, "s ⊔ zero", s.Join(zero), s)
	requireSameBytes(t, "zero ⊔ s", zero.Join(s), s)

	r.Merge(zero)
	requireElements(t, "after merging the zero value", r, "x")
	requireRejected(t, "after merging the zero value", r, 0)

	// A removal has an empty store and a context that is not: it is not
	// bottom, or it would never ship.
	r.FlushDelta()
	r.Remove("x")
	if r.Delta().IsBottom() {
		t.Fatal("a removal reads as bottom")
	}
}

func TestORSetDeltaExchangeConverges(t *testing.T) {
	a, b := newSet("a"), newSet("b")
	a.Add("x")
	a.Add("y")
	a.Remove("x")
	b.Add("x")
	b.Add("z")

	da, db := a.Delta(), b.Delta()
	a.Merge(db)
	b.Merge(da)
	requireElements(t, "a", a, "x", "y", "z")
	requireElements(t, "b", b, "x", "y", "z")
	requireSameBytes(t, "a and b", a.State(), b.State())
	requireRejected(t, "a", a, 0)
	requireRejected(t, "b", b, 0)
}

// Two adds in one buffer: the second add's context names its own dot alone,
// so the join inside the buffer does not read the first dot as superseded,
// and f travels next to e. A vector-shaped context would have claimed the
// first dot too and killed f before it ever left the replica.
func TestORSetBufferCarriesEveryAdd(t *testing.T) {
	a := newSet("a")
	a.Add("f")
	a.Add("e")

	b := newSet("b")
	b.Merge(a.Delta())
	requireElements(t, "b", b, "e", "f")
	requireRejected(t, "b", b, 0)
	requireSameBytes(t, "a and b", a.State(), b.State())
}

func TestORSetFlushDeltaDrainsTheBuffer(t *testing.T) {
	a := newSet("a")
	a.Add("x")
	before := a.Delta()
	flushed := a.FlushDelta()
	requireSameBytes(t, "the flushed delta", flushed, before)
	if !a.Delta().IsBottom() {
		t.Fatalf("the buffer is not bottom after FlushDelta: %s", bytesOf(t, a.Delta()))
	}
	requireElements(t, "a keeps its state", a, "x")

	a.Add("y")
	later := a.Delta()
	b := newSet("b")
	b.Merge(flushed)
	requireElements(t, "b after the flushed delta", b, "x")
	b.Merge(later)
	requireElements(t, "b after the later delta", b, "x", "y")
	requireRejected(t, "b", b, 0)

	// The later delta carries y alone: a replica that never saw x's add
	// cannot fold it.
	c := newSet("c")
	c.Merge(later)
	requireElements(t, "c", c)
	requireRejected(t, "c", c, 1)
}

func TestORSetSnapshotsDoNotFollowLaterOps(t *testing.T) {
	a := newSet("a")
	a.Add("x")
	state, delta := a.State(), a.Delta()
	flushed := a.FlushDelta()
	a.Add("y")
	a.Remove("x")
	requireElements(t, "a", a, "y")

	for _, s := range []struct {
		name string
		s    orsetState
	}{{"State", state}, {"Delta", delta}, {"FlushDelta", flushed}} {
		b := newSet("b")
		b.Merge(s.s)
		requireElements(t, s.name+" taken before the later ops", b, "x")
	}
}

func TestORSetObservedRemove(t *testing.T) {
	t.Run("a removal kills the adds it observed", func(t *testing.T) {
		a, b := newSet("a"), newSet("b")
		a.Add("x")
		b.Merge(a.State())
		b.Remove("x")
		a.Merge(b.Delta())
		requireElements(t, "a", a)
		requireElements(t, "b", b)
		requireSameBytes(t, "a and b", a.State(), b.State())
	})

	t.Run("a removal does not touch an add it has not seen", func(t *testing.T) {
		a, b, c := newSet("a"), newSet("b"), newSet("c")
		a.Add("x")
		c.Merge(a.Delta())
		c.Remove("x")
		removal := c.Delta()

		b.Add("x")
		b.Merge(removal)
		requireElements(t, "b after c's removal", b, "x")
		requireRejected(t, "b after c's removal", b, 0)

		a.Merge(removal)
		requireElements(t, "a after c's removal", a)

		a.Merge(b.Delta())
		c.Merge(b.Delta())
		requireElements(t, "a after b's add", a, "x")
		requireElements(t, "c after b's add", c, "x")
		requireSameBytes(t, "a and b", a.State(), b.State())
		requireSameBytes(t, "b and c", b.State(), c.State())
	})

	t.Run("a concurrent re-add wins over the removal", func(t *testing.T) {
		a, b := newSet("a"), newSet("b")
		a.Add("x")
		b.Merge(a.State())
		a.Add("x")
		b.Remove("x")

		da, db := a.Delta(), b.Delta()
		a.Merge(db)
		b.Merge(da)
		requireElements(t, "a", a, "x")
		requireElements(t, "b", b, "x")
		requireSameBytes(t, "a and b", a.State(), b.State())
		requireRejected(t, "a", a, 0)
		requireRejected(t, "b", b, 0)
	})
}

// A delta that does not continue what the replica holds is refused whole:
// the state does not change, Rejected grows, and the delta is not
// remembered — it has to arrive again once the gap is filled.
func TestORSetMergeRefusesAGappedDelta(t *testing.T) {
	a := newSet("a")
	a.Add("f")
	first := a.FlushDelta()
	a.Add("e")
	second := a.FlushDelta()

	c := newSet("c")
	before := bytesOf(t, c.State())
	c.Merge(second)
	requireElements(t, "c after the gapped delta", c)
	requireRejected(t, "c after the gapped delta", c, 1)
	if got := bytesOf(t, c.State()); !bytes.Equal(got, before) {
		t.Fatalf("a refused delta changed the state:\n was %s\n now %s", before, got)
	}

	c.Merge(second)
	requireRejected(t, "c after the gapped delta twice", c, 2)

	c.Merge(first)
	requireElements(t, "c after the first delta", c, "f")
	requireRejected(t, "c after the first delta", c, 2)

	c.Merge(second)
	requireElements(t, "c after both deltas", c, "e", "f")
	requireRejected(t, "c after both deltas", c, 2)
	requireSameBytes(t, "a and c", a.State(), c.State())
}

// A delta is refused as a whole: a dot that would fold next to one that
// would not does not get in alone.
func TestORSetMergeRefusesTheWholeDelta(t *testing.T) {
	b := newSet("b")
	b.Add("x")
	b.Add("y")
	b.Add("z")

	a := newSet("a")
	a.Add("f")
	first := a.FlushDelta()
	a.Merge(b.State())
	a.Remove("z")
	a.Add("g")
	mixed := a.FlushDelta()

	c := newSet("c")
	c.Merge(first)
	before := bytesOf(t, c.State())
	c.Merge(mixed)
	requireElements(t, "c after the mixed delta", c, "f")
	requireRejected(t, "c after the mixed delta", c, 1)
	if got := bytesOf(t, c.State()); !bytes.Equal(got, before) {
		t.Fatalf("a refused delta changed the state:\n was %s\n now %s", before, got)
	}

	c.Merge(b.State())
	c.Merge(mixed)
	requireElements(t, "c after b's state and the mixed delta", c, "f", "g", "x", "y")
	requireRejected(t, "c after b's state and the mixed delta", c, 1)
	requireSameBytes(t, "a and c", a.State(), c.State())
}

// A removal names the dots it observed. Where those dots continue the
// receiver's vector the removal folds and is accepted, and the add it
// removed arrives dead afterwards; where they do not, it is refused.
func TestORSetRemovalAheadOfItsAdd(t *testing.T) {
	a := newSet("a")
	a.Add("f")
	addF := a.FlushDelta()
	a.Add("e")
	addE := a.FlushDelta()

	b := newSet("b")
	b.Merge(addF)
	b.Merge(addE)
	b.Remove("e")
	removeE := b.FlushDelta()

	t.Run("continuing the vector, the removal is accepted and the add arrives dead", func(t *testing.T) {
		c := newSet("c")
		c.Merge(addF)
		c.Merge(removeE)
		requireElements(t, "c after the removal", c, "f")
		requireRejected(t, "c after the removal", c, 0)
		c.Merge(addE)
		requireElements(t, "c after the late add", c, "f")
		requireRejected(t, "c after the late add", c, 0)
		requireSameBytes(t, "b and c", b.State(), c.State())
	})

	t.Run("ahead of the vector, the removal is refused", func(t *testing.T) {
		d := newSet("d")
		d.Merge(removeE)
		requireElements(t, "d after the removal", d)
		requireRejected(t, "d after the removal", d, 1)
		d.Merge(addF)
		d.Merge(addE)
		requireElements(t, "d after the adds", d, "e", "f")
		d.Merge(removeE)
		requireElements(t, "d after the removal again", d, "f")
		requireRejected(t, "d after the removal again", d, 1)
		requireSameBytes(t, "b and d", b.State(), d.State())
	})
}

// Full states carry no loose dots, so a full state is never refused — in
// any order, whatever dots it names that the receiver has never seen.
func TestORSetFullStatesAreNeverRefused(t *testing.T) {
	a, b, c := newSet("a"), newSet("b"), newSet("c")
	a.Add("x")
	a.Add("y")
	b.Merge(a.State())
	b.Remove("x")
	b.Add("z")
	c.Add("x")
	c.Remove("x")
	c.Add("w")

	c.Merge(b.State())
	a.Merge(c.State())
	b.Merge(a.State())
	c.Merge(b.State())
	a.Merge(b.State())
	for _, r := range []struct {
		name string
		r    *orset
	}{{"a", a}, {"b", b}, {"c", c}} {
		requireRejected(t, r.name, r.r, 0)
		requireElements(t, r.name, r.r, "w", "y", "z")
	}
	requireSameBytes(t, "a and b", a.State(), b.State())
	requireSameBytes(t, "b and c", b.State(), c.State())
}

// Adding x again names the dots it supersedes, so the delta of a re-add is
// exact even when it spells as a vector: a replica that missed the first
// add accepts it, and the first add then arrives dead.
func TestORSetReAddSupersedesTheEarlierAdd(t *testing.T) {
	a := newSet("a")
	a.Add("x")
	first := a.FlushDelta()
	a.Add("x")
	again := a.FlushDelta()

	c := newSet("c")
	c.Merge(again)
	requireElements(t, "c after the re-add", c, "x")
	requireRejected(t, "c after the re-add", c, 0)
	c.Merge(first)
	requireElements(t, "c after the late first add", c, "x")
	requireSameBytes(t, "a and c", a.State(), c.State())
}

// The set holds anything a map can key: a struct is an element like any
// other, equal states of it still marshal to equal bytes, and they round
// trip.
func TestORSetHoldsStructElements(t *testing.T) {
	type item struct {
		SKU string
		Qty int
	}
	itemCodec := causal.ORSetJSON[item]()
	marshal := func(s causal.ORSetState[item]) []byte {
		t.Helper()
		b, err := itemCodec.Encode(s)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		return b
	}

	a, b := causal.NewORSet[item]("a"), causal.NewORSet[item]("b")
	a.Add(item{"pen", 2})
	a.Add(item{"ink", 1})
	b.Merge(a.State())
	b.Remove(item{"ink", 1})
	b.Add(item{"pen", 2})
	a.Add(item{"pad", 5})
	a.Merge(b.Delta())
	b.Merge(a.Delta())

	want := []item{{"pad", 5}, {"pen", 2}}
	for _, r := range []*causal.ORSet[item]{a, b} {
		got := r.Elements()
		slices.SortFunc(got, func(x, y item) int { return cmp.Compare(x.SKU, y.SKU) })
		if !slices.Equal(got, want) {
			t.Fatalf("elements are %v, want %v", got, want)
		}
		if r.Contains(item{"pen", 3}) {
			t.Fatal("an item with another quantity is another element, yet Contains says it is held")
		}
		if r.Rejected() != 0 {
			t.Fatalf("Rejected() = %d, want 0", r.Rejected())
		}
	}
	if x, y := marshal(a.State()), marshal(b.State()); !bytes.Equal(x, y) {
		t.Fatalf("converged replicas marshal differently:\n %s\n %s", x, y)
	}

	back, err := itemCodec.Decode(marshal(a.State()))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if x, y := marshal(back), marshal(a.State()); !bytes.Equal(x, y) {
		t.Fatalf("round trip changed the bytes:\n %s\n %s", x, y)
	}
}

// Equal states have equal wire forms, and a state at rest carries no loose
// dots.
func TestORSetWireIsCanonical(t *testing.T) {
	a, b := newSet("b"), newSet("a")
	a.Add("x")
	a.Add("y")
	b.Merge(a.State())
	b.Remove("x")
	b.Add("z")
	a.Merge(b.Delta())

	want := causal.ORSetWire[string]{
		Entries: []causal.ORSetEntry[string]{
			{Element: "z", Dots: []causal.Dot{dot("a", 1)}},
			{Element: "y", Dots: []causal.Dot{dot("b", 2)}},
		},
		VV: []causal.Dot{dot("a", 1), dot("b", 2)},
	}
	for _, r := range []struct {
		name string
		r    *orset
	}{{"a", a}, {"b", b}} {
		if got := r.r.State().Wire(); !reflect.DeepEqual(got, want) {
			t.Fatalf("wire form of %s is %+v, want %+v", r.name, got, want)
		}
	}
	requireSameBytes(t, "FromWire(Wire())", causal.ORSetStateFromWire(want), a.State())

	if w := (orsetState{}).Wire(); w.Entries != nil || w.VV != nil || w.Dots != nil {
		t.Fatalf("bottom has a wire form with content: %+v", w)
	}
	if !causal.ORSetStateFromWire(causal.ORSetWire[string]{}).IsBottom() {
		t.Fatal("the zero wire form is not bottom")
	}
}

// Equal states marshal to equal bytes, whatever path built them and
// however the maps iterate.
func TestORSetStateBytesAreCanonical(t *testing.T) {
	a, b, c := newSet("a"), newSet("b"), newSet("c")
	a.Add("x")
	a.Add("y")
	b.Merge(a.State())
	b.Remove("x")
	b.Add("z")
	c.Add("w")
	c.Add("x")

	x := a.State().Join(b.State()).Join(c.State())
	y := c.State().Join(b.State().Join(a.State()))
	requireSameBytes(t, "(a⊔b)⊔c and c⊔(b⊔a)", x, y)

	first := bytesOf(t, x)
	for range 20 {
		if got := bytesOf(t, x); !bytes.Equal(got, first) {
			t.Fatalf("marshal is not stable:\n %s\n %s", first, got)
		}
	}

	d := newSet("d")
	d.Merge(a.State())
	d.Merge(b.State())
	d.Merge(c.State())
	requireSameBytes(t, "merged one by one and joined at once", d.State(), x)
}

func TestORSetStateRoundTrip(t *testing.T) {
	roundTrip := func(t *testing.T, s orsetState) orsetState {
		t.Helper()
		back, err := codec.Decode(bytesOf(t, s))
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		requireSameBytes(t, "after the round trip", back, s)
		if back.IsBottom() != s.IsBottom() {
			t.Fatalf("IsBottom changed across the round trip: %v from %v", back.IsBottom(), s.IsBottom())
		}
		return back
	}

	t.Run("zero value", func(t *testing.T) {
		var zero orsetState
		back := roundTrip(t, zero)
		if !back.IsBottom() {
			t.Fatal("the decoded zero value is not bottom")
		}
	})

	t.Run("state at rest", func(t *testing.T) {
		a := newSet("a")
		a.Add("x")
		a.Add("y")
		a.Remove("x")
		b := newSet("b")
		b.Add("x")
		a.Merge(b.State())
		back := roundTrip(t, a.State())

		c := newSet("c")
		c.Merge(back)
		requireElements(t, "c", c, "x", "y")
		requireRejected(t, "c", c, 0)
		requireSameBytes(t, "a and c", a.State(), c.State())
	})

	// A delta must come back with its loose dots loose: a codec that spelled
	// them as a vector would claim dots the delta never carried.
	t.Run("a delta keeps its loose dots", func(t *testing.T) {
		a := newSet("a")
		a.Add("f")
		first := a.FlushDelta()
		a.Add("e")
		second := roundTrip(t, a.FlushDelta())

		c := newSet("c")
		c.Merge(second)
		requireElements(t, "c after the decoded gapped delta", c)
		requireRejected(t, "c after the decoded gapped delta", c, 1)

		c.Merge(first)
		c.Merge(second)
		requireElements(t, "c after both", c, "e", "f")
		requireSameBytes(t, "a and c", a.State(), c.State())
	})

	t.Run("a removal keeps its context", func(t *testing.T) {
		a := newSet("a")
		a.Add("x")
		add := a.FlushDelta()
		a.Remove("x")
		removal := roundTrip(t, a.FlushDelta())
		if removal.IsBottom() {
			t.Fatal("the decoded removal is bottom")
		}

		c := newSet("c")
		c.Merge(add)
		c.Merge(removal)
		requireElements(t, "c after the decoded removal", c)
		requireRejected(t, "c after the decoded removal", c, 0)
		requireSameBytes(t, "a and c", a.State(), c.State())
	})

	t.Run("garbage is an error", func(t *testing.T) {
		if _, err := codec.Decode([]byte("not a state")); err == nil {
			t.Fatal("garbage decoded without an error")
		}
	})
}

// The replica is touched from several goroutines in the engine: user ops,
// the gossip loop's FlushDelta, inbound Merge. No update may be lost and no
// map may be touched unguarded — run with -race for the full check.
func TestORSetConcurrent(t *testing.T) {
	const writers, perWriter = 8, 500
	a := newSet("a")
	peer := newSet("peer")
	peer.Add("peer")
	peerState := peer.State()

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				_ = a.FlushDelta()
				_ = a.State()
				_ = a.Elements()
				_ = a.Contains("w0-1")
				a.Merge(peerState)
				_ = a.Rejected()
			}
		}
	}()

	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			for i := range perWriter {
				e := fmt.Sprintf("w%d-%d", w, i)
				a.Add(e)
				if i%2 == 0 {
					a.Remove(e)
				}
			}
		})
	}
	wg.Wait()
	close(stop)
	<-done
	a.Merge(peerState)

	want := []string{"peer"}
	for w := range writers {
		for i := 1; i < perWriter; i += 2 {
			want = append(want, fmt.Sprintf("w%d-%d", w, i))
		}
	}
	slices.Sort(want)
	got := sortedElements(a)
	if !slices.Equal(got, want) {
		t.Fatalf("elements after concurrent ops: %d elements, want %d; first difference at %d",
			len(got), len(want), firstDifference(got, want))
	}
	requireRejected(t, "a", a, 0)
}

func firstDifference(a, b []string) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}
