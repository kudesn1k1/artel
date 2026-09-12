package causal_test

import (
	"slices"
	"testing"

	"pgregory.net/rapid"

	"github.com/kudesn1k1/artel/internal/causal"
)

// Property tests for the delta OR-Set. States and deltas come only from real
// operations on a small crew of replicas — there is no other way to build
// one — and every generator draws in a fixed order so a failing seed replays
// the same values. Equality is equality of marshaled bytes: canonical bytes
// are part of the contract, the simulator's convergence check compares
// nothing else.

// other draws a replica index different from i.
func other(t *rapid.T, i, n int) int {
	return (i + 1 + rapid.IntRange(0, n-2).Draw(t, "other")) % n
}

// genWorld runs a random history over the crew and collects every value it
// produced: states at rest, buffered deltas, and flushed deltas — some of
// them dropped on the floor so the next delta of that replica starts
// mid-run and carries loose dots.
func genWorld(t *rapid.T) (reps []*orset, pool []orsetState) {
	for _, id := range replicas {
		reps = append(reps, newSet(id))
	}
	n := len(reps)
	steps := rapid.IntRange(0, 16).Draw(t, "steps")
	for range steps {
		i := rapid.IntRange(0, n-1).Draw(t, "replica")
		switch rapid.IntRange(0, 4).Draw(t, "op") {
		case 0:
			reps[i].Add(rapid.SampledFrom(elements).Draw(t, "element"))
		case 1:
			reps[i].Remove(rapid.SampledFrom(elements).Draw(t, "element"))
		case 2:
			reps[other(t, i, n)].Merge(reps[i].State())
		case 3:
			pool = append(pool, reps[i].FlushDelta())
		case 4:
			d := reps[i].FlushDelta()
			pool = append(pool, d)
			for j := range reps {
				if j != i {
					reps[j].Merge(d)
				}
			}
		}
	}
	for _, r := range reps {
		pool = append(pool, r.State(), r.Delta())
	}
	return reps, pool
}

func pick(t *rapid.T, pool []orsetState, label string) orsetState {
	return rapid.SampledFrom(pool).Draw(t, label)
}

func TestORSetJoinLaws(t *testing.T) {
	t.Run("commutative", rapid.MakeCheck(func(t *rapid.T) {
		_, pool := genWorld(t)
		x, y := pick(t, pool, "x"), pick(t, pool, "y")
		requireSameBytes(t, "x⊔y = y⊔x", x.Join(y), y.Join(x))
	}))
	t.Run("associative", rapid.MakeCheck(func(t *rapid.T) {
		_, pool := genWorld(t)
		x, y, z := pick(t, pool, "x"), pick(t, pool, "y"), pick(t, pool, "z")
		requireSameBytes(t, "(x⊔y)⊔z = x⊔(y⊔z)", x.Join(y).Join(z), x.Join(y.Join(z)))
	}))
	t.Run("idempotent", rapid.MakeCheck(func(t *rapid.T) {
		_, pool := genWorld(t)
		x, y := pick(t, pool, "x"), pick(t, pool, "y")
		requireSameBytes(t, "x⊔x = x", x.Join(x), x)
		xy := x.Join(y)
		requireSameBytes(t, "(x⊔y)⊔y = x⊔y", xy.Join(y), xy)
	}))
	t.Run("bottom is the identity", rapid.MakeCheck(func(t *rapid.T) {
		_, pool := genWorld(t)
		x := pick(t, pool, "x")
		var zero orsetState
		requireSameBytes(t, "x⊔⊥ = x", x.Join(zero), x)
		requireSameBytes(t, "⊥⊔x = x", zero.Join(x), x)
	}))
	// Join builds fresh values: the inputs come out as they went in, even
	// after the result has been merged into a replica that keeps mutating.
	t.Run("pure", rapid.MakeCheck(func(t *rapid.T) {
		_, pool := genWorld(t)
		x, y := pick(t, pool, "x"), pick(t, pool, "y")
		xBefore, yBefore := bytesOf(t, x), bytesOf(t, y)
		z := x.Join(y)
		r := newSet("q")
		r.Merge(z)
		r.Add("q")
		r.Remove(rapid.SampledFrom(elements).Draw(t, "element"))
		_ = r.FlushDelta()
		if got := bytesOf(t, x); !slices.Equal(got, xBefore) {
			t.Fatalf("x changed after the join:\n was %s\n now %s", xBefore, got)
		}
		if got := bytesOf(t, y); !slices.Equal(got, yBefore) {
			t.Fatalf("y changed after the join:\n was %s\n now %s", yBefore, got)
		}
	}))
	t.Run("round trip", rapid.MakeCheck(func(t *rapid.T) {
		_, pool := genWorld(t)
		x, y := pick(t, pool, "x"), pick(t, pool, "y")
		back, err := codec.Decode(bytesOf(t, x))
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		requireSameBytes(t, "decoded x", back, x)
		if back.IsBottom() != x.IsBottom() {
			t.Fatalf("IsBottom changed across the round trip: %v from %v", back.IsBottom(), x.IsBottom())
		}
		requireSameBytes(t, "decoded x ⊔ y", back.Join(y), x.Join(y))
	}))
	// Merge is the join guarded by the causal check: either the delta gets
	// in whole and the state is the join, or it is refused whole, counted,
	// and the state is untouched.
	t.Run("merge is the join or nothing", rapid.MakeCheck(func(t *rapid.T) {
		reps, pool := genWorld(t)
		r := reps[rapid.IntRange(0, len(reps)-1).Draw(t, "replica")]
		d := pick(t, pool, "d")
		before, rejected := r.State(), r.Rejected()
		r.Merge(d)
		switch r.Rejected() {
		case rejected:
			requireSameBytes(t, "accepted: state = state ⊔ d", r.State(), before.Join(d))
		case rejected + 1:
			requireSameBytes(t, "refused: state unchanged", r.State(), before)
		default:
			t.Fatalf("Rejected() went from %d to %d on one Merge", rejected, r.Rejected())
		}
	}))
}

// modelSet is the textbook observed-remove set with tombstones: an element
// is present while one of its tags is not tombstoned. Adding tombstones the
// tags it supersedes, as the delta type does, so a model delta is the tags
// minted and the tags tombstoned since the last flush. Tombstones make it
// order-independent, which is why it can judge the real type only where the
// real type refuses nothing.
type modelSet struct {
	id        string
	next      uint64
	tags      map[string]map[causal.Dot]bool
	dead      map[causal.Dot]bool
	deltaTags map[string]map[causal.Dot]bool
	deltaDead map[causal.Dot]bool
}

type modelDelta struct {
	tags map[string]map[causal.Dot]bool
	dead map[causal.Dot]bool
}

func newModel(id string) *modelSet {
	return &modelSet{
		id:        id,
		tags:      map[string]map[causal.Dot]bool{},
		dead:      map[causal.Dot]bool{},
		deltaTags: map[string]map[causal.Dot]bool{},
		deltaDead: map[causal.Dot]bool{},
	}
}

func (m *modelSet) live(e string) []causal.Dot {
	var out []causal.Dot
	for tag := range m.tags[e] {
		if !m.dead[tag] {
			out = append(out, tag)
		}
	}
	return out
}

func (m *modelSet) tombstone(tags []causal.Dot) {
	for _, tag := range tags {
		m.dead[tag] = true
		m.deltaDead[tag] = true
	}
}

func (m *modelSet) add(e string) {
	old := m.live(e)
	m.next++
	tag := causal.Dot{Replica: m.id, N: m.next}
	for _, tags := range []map[string]map[causal.Dot]bool{m.tags, m.deltaTags} {
		if tags[e] == nil {
			tags[e] = map[causal.Dot]bool{}
		}
		tags[e][tag] = true
	}
	m.tombstone(old)
}

func (m *modelSet) remove(e string) {
	m.tombstone(m.live(e))
}

func (m *modelSet) elements() []string {
	var out []string
	for e := range m.tags {
		if len(m.live(e)) > 0 {
			out = append(out, e)
		}
	}
	slices.Sort(out)
	return out
}

func cloneTags(tags map[string]map[causal.Dot]bool) map[string]map[causal.Dot]bool {
	out := make(map[string]map[causal.Dot]bool, len(tags))
	for e, set := range tags {
		out[e] = make(map[causal.Dot]bool, len(set))
		for tag := range set {
			out[e][tag] = true
		}
	}
	return out
}

func cloneDead(dead map[causal.Dot]bool) map[causal.Dot]bool {
	out := make(map[causal.Dot]bool, len(dead))
	for tag := range dead {
		out[tag] = true
	}
	return out
}

func (m *modelSet) state() modelDelta {
	return modelDelta{cloneTags(m.tags), cloneDead(m.dead)}
}

func (m *modelSet) flush() modelDelta {
	d := modelDelta{m.deltaTags, m.deltaDead}
	m.deltaTags = map[string]map[causal.Dot]bool{}
	m.deltaDead = map[causal.Dot]bool{}
	return d
}

func (m *modelSet) merge(d modelDelta) {
	for e, set := range d.tags {
		if m.tags[e] == nil {
			m.tags[e] = map[causal.Dot]bool{}
		}
		for tag := range set {
			m.tags[e][tag] = true
		}
	}
	for tag := range d.dead {
		m.dead[tag] = true
	}
}

// Two state machines drive the crew and the model in lockstep and compare
// membership after every step. Each keeps to a delivery pattern under which
// the real type refuses nothing: full states in any order, or every flushed
// delta handed to everyone at once. Mixing the two would let a removal name
// dots a peer never saw, and the real type would rightly refuse where the
// model cannot.
func TestORSetAgreesWithTheTombstoneModel(t *testing.T) {
	newCrew := func() (reps []*orset, models []*modelSet) {
		for _, id := range replicas {
			reps = append(reps, newSet(id))
			models = append(models, newModel(id))
		}
		return reps, models
	}
	pickReplica := func(t *rapid.T) int {
		return rapid.IntRange(0, len(replicas)-1).Draw(t, "replica")
	}
	pickElement := func(t *rapid.T) string {
		return rapid.SampledFrom(elements).Draw(t, "element")
	}
	check := func(reps []*orset, models []*modelSet) func(*rapid.T) {
		return func(t *rapid.T) {
			for i := range reps {
				if got, want := sortedElements(reps[i]), models[i].elements(); !slices.Equal(got, want) {
					t.Fatalf("%s holds %v, the model holds %v", replicas[i], got, want)
				}
				if n := reps[i].Rejected(); n != 0 {
					t.Fatalf("%s refused %d deltas under in-order delivery", replicas[i], n)
				}
			}
		}
	}
	add := func(reps []*orset, models []*modelSet) func(*rapid.T) {
		return func(t *rapid.T) {
			i, e := pickReplica(t), pickElement(t)
			reps[i].Add(e)
			models[i].add(e)
		}
	}
	remove := func(reps []*orset, models []*modelSet) func(*rapid.T) {
		return func(t *rapid.T) {
			i, e := pickReplica(t), pickElement(t)
			reps[i].Remove(e)
			models[i].remove(e)
		}
	}

	t.Run("full-state exchange", rapid.MakeCheck(func(t *rapid.T) {
		reps, models := newCrew()
		t.Repeat(map[string]func(*rapid.T){
			"":       check(reps, models),
			"add":    add(reps, models),
			"remove": remove(reps, models),
			"sync": func(t *rapid.T) {
				i := pickReplica(t)
				j := other(t, i, len(reps))
				reps[j].Merge(reps[i].State())
				models[j].merge(models[i].state())
			},
		})
	}))

	t.Run("delta broadcast", rapid.MakeCheck(func(t *rapid.T) {
		reps, models := newCrew()
		t.Repeat(map[string]func(*rapid.T){
			"":       check(reps, models),
			"add":    add(reps, models),
			"remove": remove(reps, models),
			"broadcast": func(t *rapid.T) {
				i := pickReplica(t)
				d, md := reps[i].FlushDelta(), models[i].flush()
				for j := range reps {
					if j != i {
						reps[j].Merge(d)
						models[j].merge(md)
					}
				}
			},
		})
	}))
}
