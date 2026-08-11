package set

import (
	"testing"

	"crdtlab/crdt"
)

// ORSet contract assumed by these tests (align your impl to this):
//   NewORSet[E comparable](id string) *ORSet[E]
//   (*ORSet[E]).Add(e E)
//   (*ORSet[E]).Remove(e E)        // removes only the dots observed now
//   (*ORSet[E]).Contains(e E) bool
//   (*ORSet[E]).Value() []E        // distinct elements with >=1 live dot
//   (*ORSet[E]).Merge(other ORSetState[E])
//   state type: ORSetState[E comparable]
//
// State shape is the same dot-store as the optimized MV register:
//   { dots map[VersionDot]E ; cc VersionVector }

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]int)
	for _, x := range a {
		m[x]++
	}
	for _, x := range b {
		m[x]--
	}
	for _, c := range m {
		if c != 0 {
			return false
		}
	}
	return true
}

func TestORSet(t *testing.T) {
	t.Run("add then contains", func(t *testing.T) {
		s := NewORSet[string]("A")
		s.Add("x")
		if !s.Contains("x") {
			t.Fatal("x should be present after Add")
		}
		if got := s.Value(); !sameSet(got, []string{"x"}) {
			t.Fatalf("Value() = %v, want [x]", got)
		}
	})

	t.Run("add then remove is absent", func(t *testing.T) {
		s := NewORSet[string]("A")
		s.Add("x")
		s.Remove("x")
		if s.Contains("x") {
			t.Fatal("x should be absent after Remove")
		}
		if got := s.Value(); !sameSet(got, []string{}) {
			t.Fatalf("Value() = %v, want []", got)
		}
	})

	t.Run("re-add after remove works (unlike 2P-Set)", func(t *testing.T) {
		s := NewORSet[string]("A")
		s.Add("x")
		s.Remove("x")
		s.Add("x") // fresh dot the remove never observed
		if !s.Contains("x") {
			t.Fatal("re-add must bring x back")
		}
	})

	t.Run("concurrent add wins over remove (observed-remove)", func(t *testing.T) {
		a := NewORSet[string]("A")
		b := NewORSet[string]("B")
		a.Add("x")
		b.Merge(a.State()) // both observe a's dot for x

		// concurrent: A removes the dot it observed; B adds a fresh dot for x
		a.Remove("x")
		b.Add("x")

		// A receives B's concurrent state. The old observed dot stays removed,
		// but B's fresh, unobserved dot survives -> x is present (add wins).
		// One-directional on purpose: a true bidirectional in-memory exchange
		// would need independent snapshots, which is the wire's job (v0.1), not
		// the type's. Bidirectional convergence is covered by the join laws.
		a.Merge(b.State())
		if !a.Contains("x") {
			t.Fatal("concurrent add must survive a concurrent remove (x absent)")
		}
	})

	t.Run("converges across replicas", func(t *testing.T) {
		net := crdt.NewNetwork[*ORSet[string], ORSetState[string]]()
		net.Add("A", NewORSet[string]("A"))
		net.Add("B", NewORSet[string]("B"))
		net.Add("C", NewORSet[string]("C"))

		net.Get("A").Add("x")
		net.Get("A").Add("y")
		net.Get("B").Add("z")
		net.Sync("A", "B")
		net.Get("B").Remove("x") // B removes x after observing it from A
		net.Get("C").Add("w")

		net.SyncAll()

		want := []string{"y", "z", "w"} // x removed after being observed
		for _, id := range []string{"A", "B", "C"} {
			if got := net.Get(id).Value(); !sameSet(got, want) {
				t.Errorf("replica %s: Value() = %v, want %v", id, got, want)
			}
		}
	})

	t.Run("observed-remove with multiple dots; concurrent add survives", func(t *testing.T) {
		a := NewORSet[string]("A")
		b := NewORSet[string]("B")
		a.Add("x") // dot (A,1)
		b.Add("x") // dot (B,1)

		c := NewORSet[string]("C")
		c.Merge(a.State())
		c.Merge(b.State()) // c sees x carried by TWO dots
		c.Remove("x")      // removes both observed dots

		d := NewORSet[string]("D")
		d.Add("x") // a fresh dot c never observed

		c.Merge(d.State())
		if !c.Contains("x") {
			t.Fatal("a concurrent add must survive removal of the observed dots")
		}
		// and the old observed dots must not resurrect / duplicate the element
		c.Merge(a.State())
		if got := c.Value(); !sameSet(got, []string{"x"}) {
			t.Fatalf("old dots resurrected or element duplicated: %v", got)
		}
	})

	t.Run("merge is commutative", func(t *testing.T) {
		a := NewORSet[string]("A")
		a.Add("x")
		b := NewORSet[string]("B")
		b.Add("y")
		c := NewORSet[string]("C")
		c.Add("z")

		left := NewORSet[string]("L")
		left.Merge(a.State())
		left.Merge(b.State())
		left.Merge(c.State())
		right := NewORSet[string]("R")
		right.Merge(c.State())
		right.Merge(b.State())
		right.Merge(a.State())

		if !sameSet(left.Value(), right.Value()) {
			t.Fatalf("commutativity: left=%v right=%v", left.Value(), right.Value())
		}
		if !sameSet(left.Value(), []string{"x", "y", "z"}) {
			t.Fatalf("want [x y z], got %v", left.Value())
		}
	})

	t.Run("merge is idempotent", func(t *testing.T) {
		a := NewORSet[string]("A")
		a.Add("x")
		a.Add("y")

		once := NewORSet[string]("T")
		once.Merge(a.State())

		twice := NewORSet[string]("T")
		twice.Merge(a.State())
		twice.Merge(a.State())

		if !sameSet(once.Value(), twice.Value()) {
			t.Fatalf("idempotence: once=%v twice=%v", once.Value(), twice.Value())
		}
	})
}
