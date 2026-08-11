package register

import (
	"testing"

	"crdtlab/crdt"
)

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

func TestMVRegister(t *testing.T) {
	t.Run("concurrent writes both survive as siblings", func(t *testing.T) {
		net := crdt.NewNetwork[*MVRegister[string], MVRegisterState[string]]()
		net.Add("A", NewMVRegister[string]("A"))
		net.Add("B", NewMVRegister[string]("B"))

		net.Get("A").Set("a") // concurrent: neither has seen the other
		net.Get("B").Set("b")
		net.SyncAll()

		want := []string{"a", "b"}
		for _, id := range []string{"A", "B"} {
			if got := net.Get(id).Value(); !sameSet(got, want) {
				t.Errorf("replica %s: Value() = %v, want siblings %v", id, got, want)
			}
		}
	})

	t.Run("causally later write leaves no sibling", func(t *testing.T) {
		a := NewMVRegister[string]("A")
		b := NewMVRegister[string]("B")

		a.Set("a")
		b.Merge(a.State()) // B observes a (causal, not concurrent)
		b.Set("b")         // b's version vector dominates a's
		a.Merge(b.State())

		if got := a.Value(); !sameSet(got, []string{"b"}) {
			t.Fatalf("causal overwrite should drop a; got %v, want [b]", got)
		}
	})

	t.Run("a fresh write supersedes existing siblings", func(t *testing.T) {
		a := NewMVRegister[string]("A")
		b := NewMVRegister[string]("B")
		a.Set("a")
		b.Set("b")

		r := NewMVRegister[string]("R")
		r.Merge(a.State())
		r.Merge(b.State()) // r now holds siblings {a, b}
		if got := r.Value(); !sameSet(got, []string{"a", "b"}) {
			t.Fatalf("setup: r should hold {a,b}, got %v", got)
		}

		r.Set("c") // dominates both siblings -> collapses to {c}
		if got := r.Value(); !sameSet(got, []string{"c"}) {
			t.Fatalf("fresh write should supersede siblings; got %v, want [c]", got)
		}
	})

	t.Run("a superseded value does not resurrect from a stale replica", func(t *testing.T) {
		a := NewMVRegister[string]("A")
		stale := NewMVRegister[string]("S")

		a.Set("v1")
		stale.Merge(a.State()) // stale now holds v1

		a.Set("v2") // supersedes v1 on A; A's causal context remembers v1's dot

		// stale still carries v1; merging it must NOT bring v1 back.
		a.Merge(stale.State())
		if got := a.Value(); !sameSet(got, []string{"v2"}) {
			t.Fatalf("v1 resurrected from a stale replica: got %v, want [v2]", got)
		}
	})

	t.Run("merge is idempotent", func(t *testing.T) {
		a := NewMVRegister[string]("A")
		b := NewMVRegister[string]("B")
		a.Set("a")
		b.Set("b")

		once := NewMVRegister[string]("X")
		once.Merge(a.State())
		once.Merge(b.State())

		twice := NewMVRegister[string]("Y")
		twice.Merge(a.State())
		twice.Merge(b.State())
		twice.Merge(a.State())
		twice.Merge(b.State())

		if !sameSet(once.Value(), twice.Value()) {
			t.Fatalf("idempotence: once=%v twice=%v", once.Value(), twice.Value())
		}
	})

	t.Run("merge is commutative", func(t *testing.T) {
		a := NewMVRegister[string]("A")
		b := NewMVRegister[string]("B")
		c := NewMVRegister[string]("C")
		a.Set("a")
		b.Set("b")
		c.Set("c") // three concurrent writes

		left := NewMVRegister[string]("L")
		left.Merge(a.State())
		left.Merge(b.State())
		left.Merge(c.State())

		right := NewMVRegister[string]("R")
		right.Merge(c.State())
		right.Merge(a.State())
		right.Merge(b.State())

		if !sameSet(left.Value(), right.Value()) {
			t.Fatalf("commutativity: left=%v right=%v", left.Value(), right.Value())
		}
		if !sameSet(left.Value(), []string{"a", "b", "c"}) {
			t.Fatalf("three concurrent writes should all survive; got %v", left.Value())
		}
	})
}
