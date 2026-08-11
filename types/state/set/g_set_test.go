package set

import (
	"reflect"
	"testing"

	"crdtlab/crdt"
)

func TestGSet(t *testing.T) {
	t.Run("add and list (duplicate add is a no-op)", func(t *testing.T) {
		s := NewGSet[string]("A")
		s.Add("x")
		s.Add("y")
		s.Add("x")
		if got := s.Values(); !sameSet(got, []string{"x", "y"}) {
			t.Fatalf("Values() = %v, want [x y]", got)
		}
	})

	t.Run("converges to the union across replicas", func(t *testing.T) {
		net := crdt.NewNetwork[*GSet[string], GSetState[string]]()
		net.Add("A", NewGSet[string]("A"))
		net.Add("B", NewGSet[string]("B"))
		net.Add("C", NewGSet[string]("C"))

		net.Get("A").Add("x")
		net.Get("A").Add("y")
		net.Get("B").Add("y") // overlap with A
		net.Get("B").Add("z")
		net.Get("C").Add("w")
		net.Sync("A", "B")
		net.SyncAll()

		want := []string{"x", "y", "z", "w"}
		for _, id := range []string{"A", "B", "C"} {
			if got := net.Get(id).Values(); !sameSet(got, want) {
				t.Errorf("replica %s: Values() = %v, want %v", id, got, want)
			}
		}
	})

	t.Run("merge is idempotent", func(t *testing.T) {
		src := NewGSet[string]("S")
		src.Add("a")
		src.Add("b")

		once := NewGSet[string]("T")
		once.Merge(src.State())
		twice := NewGSet[string]("T")
		twice.Merge(src.State())
		twice.Merge(src.State())
		if !reflect.DeepEqual(once.State(), twice.State()) {
			t.Fatalf("idempotence: once=%v twice=%v", once.Values(), twice.Values())
		}
	})

	t.Run("merge is commutative", func(t *testing.T) {
		x := NewGSet[string]("X")
		x.Add("x")
		y := NewGSet[string]("Y")
		y.Add("y")
		z := NewGSet[string]("Z")
		z.Add("z")

		left := NewGSet[string]("L")
		left.Merge(x.State())
		left.Merge(y.State())
		left.Merge(z.State())
		right := NewGSet[string]("R")
		right.Merge(z.State())
		right.Merge(y.State())
		right.Merge(x.State())
		if !sameSet(left.Values(), right.Values()) {
			t.Fatalf("commutativity: left=%v right=%v", left.Values(), right.Values())
		}
	})
}
