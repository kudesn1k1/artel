package graph

import (
	"crdtlab/utils"
	"testing"

	"crdtlab/crdt"
)

func equalSeq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestRGA(t *testing.T) {
	var head utils.HLCDot // zero value

	t.Run("insert builds a sequence in order", func(t *testing.T) {
		now := func() int64 { return 1 }
		r := NewRGA[string]("A", now)
		a := r.InsertAfter(head, "a")
		b := r.InsertAfter(a, "b")
		r.InsertAfter(b, "c")
		if got := r.Value(); !equalSeq(got, []string{"a", "b", "c"}) {
			t.Fatalf("Value() = %v, want [a b c]", got)
		}
	})

	t.Run("delete tombstones: hidden but still a valid anchor", func(t *testing.T) {
		// The point of a tombstone (vs real removal): a concurrent insert anchored
		// at the deleted node must keep its anchor, so the sequence stays well-formed.
		now := func() int64 { return 1 }
		a := NewRGA[string]("A", now)
		b := NewRGA[string]("B", now)

		a1 := a.InsertAfter(head, "a")
		b.Merge(a.State()) // B learns a1

		// concurrent: A deletes a1; B inserts "b" anchored AT a1
		a.Delete(a1)
		b.InsertAfter(a1, "b")

		a.Merge(b.State())
		b.Merge(a.State())

		want := []string{"b"} // 'a' tombstoned (hidden); 'b' survives, anchored to it
		for name, r := range map[string]*RGA[string]{"A": a, "B": b} {
			if got := r.Value(); !equalSeq(got, want) {
				t.Fatalf("replica %s: Value() = %v, want %v (deleted node must stay anchorable)", name, got, want)
			}
		}
	})

	t.Run("forward-typed runs do not interleave", func(t *testing.T) {
		// A types "ab" forward (b chained after a); B types "xy" forward; concurrent.
		// Only the run-heads (a, x) share the head anchor; the tails live in subtrees,
		// so DFS emits each run whole. The runs may appear in either order, but never
		// interleaved.
		now := func() int64 { return 1 }
		a := NewRGA[string]("A", now)
		b := NewRGA[string]("B", now)

		a1 := a.InsertAfter(head, "a")
		a.InsertAfter(a1, "b")
		b1 := b.InsertAfter(head, "x")
		b.InsertAfter(b1, "y")

		a.Merge(b.State())
		b.Merge(a.State())

		got := a.Value()
		if !equalSeq(got, []string{"x", "y", "a", "b"}) && !equalSeq(got, []string{"a", "b", "x", "y"}) {
			t.Fatalf("runs interleaved: Value() = %v, want [x y a b] or [a b x y]", got)
		}
		if !equalSeq(a.Value(), b.Value()) {
			t.Fatalf("replicas diverged: a=%v b=%v", a.Value(), b.Value())
		}
	})

	t.Run("fixed-position concurrent inserts interleave (documented anomaly)", func(t *testing.T) {
		// Both replicas insert two chars at the SAME position (after head, cursor not
		// advancing) -> all four become siblings of head -> the tie-break interleaves
		// them. This is EXPECTED RGA behavior, pinned on purpose, not a bug.
		now := func() int64 { return 1 }
		a := NewRGA[string]("A", now)
		b := NewRGA[string]("B", now)

		a.InsertAfter(head, "a") // (1,0,A)
		a.InsertAfter(head, "b") // (1,1,A)
		b.InsertAfter(head, "x") // (1,0,B)
		b.InsertAfter(head, "y") // (1,1,B)

		a.Merge(b.State())
		b.Merge(a.State())

		// Descending by (L,C,Replica): (1,1,B)=y, (1,1,A)=b, (1,0,B)=x, (1,0,A)=a.
		// (With an ascending tie-break you'd get [a x b y] instead — your original
		// "axby" guess; both interleave, only the direction differs.)
		want := []string{"y", "b", "x", "a"}
		if got := a.Value(); !equalSeq(got, want) {
			t.Fatalf("Value() = %v, want %v (interleaving anomaly, descending tie-break)", got, want)
		}
		if !equalSeq(a.Value(), b.Value()) {
			t.Fatalf("replicas diverged: a=%v b=%v", a.Value(), b.Value())
		}
	})

	t.Run("converges across replicas with inserts and deletes", func(t *testing.T) {
		now := func() int64 { return 1 }
		net := crdt.NewNetwork[*RGA[string], RGAState[string]]()
		net.Add("A", NewRGA[string]("A", now))
		net.Add("B", NewRGA[string]("B", now))
		net.Add("C", NewRGA[string]("C", now))

		a1 := net.Get("A").InsertAfter(head, "h")
		a2 := net.Get("A").InsertAfter(a1, "i")
		net.SyncAll() // everyone has "hi"

		net.Get("B").InsertAfter(a2, "!") // B appends after 'i'
		net.Get("C").Delete(a1)           // C deletes 'h' concurrently
		net.SyncAll()

		if !net.Converged() {
			t.Fatal("replicas did not converge")
		}
		want := []string{"i", "!"} // 'h' tombstoned; 'i' then '!'
		for _, id := range []string{"A", "B", "C"} {
			if got := net.Get(id).Value(); !equalSeq(got, want) {
				t.Errorf("replica %s: Value() = %v, want %v", id, got, want)
			}
		}
	})

	t.Run("merge is idempotent", func(t *testing.T) {
		now := func() int64 { return 1 }
		src := NewRGA[string]("S", now)
		s1 := src.InsertAfter(head, "a")
		src.InsertAfter(s1, "b")

		once := NewRGA[string]("T", now)
		once.Merge(src.State())
		twice := NewRGA[string]("T", now)
		twice.Merge(src.State())
		twice.Merge(src.State())

		if !equalSeq(once.Value(), twice.Value()) {
			t.Fatalf("idempotence: once=%v twice=%v", once.Value(), twice.Value())
		}
	})

	t.Run("merge is commutative", func(t *testing.T) {
		now := func() int64 { return 1 }
		a := NewRGA[string]("A", now)
		a.InsertAfter(head, "a")
		b := NewRGA[string]("B", now)
		b.InsertAfter(head, "b")
		c := NewRGA[string]("C", now)
		c.InsertAfter(head, "c")

		left := NewRGA[string]("L", now)
		left.Merge(a.State())
		left.Merge(b.State())
		left.Merge(c.State())
		right := NewRGA[string]("R", now)
		right.Merge(c.State())
		right.Merge(b.State())
		right.Merge(a.State())

		if !equalSeq(left.Value(), right.Value()) {
			t.Fatalf("commutativity: left=%v right=%v", left.Value(), right.Value())
		}
	})
}
