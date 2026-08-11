package counter

import (
	"reflect"
	"testing"

	"crdtlab/crdt"
)

func samePN(a, b *PNCounter) bool { return reflect.DeepEqual(a.State(), b.State()) }

func TestPNCounter(t *testing.T) {
	t.Run("converges and counts, incl. negative", func(t *testing.T) {
		net := crdt.NewNetwork[*PNCounter, PNCounterState]()
		for _, id := range []string{"A", "B", "C"} {
			net.Add(id, NewPNCounter(id))
		}
		// A: +2 ; B: +1, -3 ; C: -2  =>  (2+1) - (3+2) = -2
		net.Get("A").Increment()
		net.Get("A").Increment()
		net.Get("B").Increment()
		net.Get("B").Decrement()
		net.Get("B").Decrement()
		net.Get("B").Decrement()
		net.Get("C").Decrement()
		net.Get("C").Decrement()

		net.Sync("A", "B")
		net.Sync("C", "A")
		net.SyncAll()

		if !net.Converged() {
			t.Fatal("replicas did not converge")
		}
		for _, id := range []string{"A", "B", "C"} {
			if got := net.Get(id).Value(); got != -2 {
				t.Errorf("replica %s: Value() = %d, want -2", id, got)
			}
		}
	})

	t.Run("merge is idempotent", func(t *testing.T) {
		src := NewPNCounter("S")
		src.Increment()
		src.Decrement()

		once := NewPNCounter("T")
		once.Merge(src.State())

		twice := NewPNCounter("T")
		twice.Merge(src.State())
		twice.Merge(src.State())

		if !samePN(once, twice) {
			t.Fatalf("merging twice != once:\n once  = %+v\n twice = %+v", once.State(), twice.State())
		}
	})

	t.Run("merge is commutative and associative", func(t *testing.T) {
		x := NewPNCounter("X")
		x.Increment()
		x.Increment()
		y := NewPNCounter("Y")
		y.Decrement()
		z := NewPNCounter("Z")
		z.Increment()
		z.Decrement()
		z.Decrement()

		left := NewPNCounter("L")
		left.Merge(x.State())
		left.Merge(y.State())
		left.Merge(z.State())

		right := NewPNCounter("R")
		right.Merge(z.State())
		right.Merge(y.State())
		right.Merge(x.State())

		if !samePN(left, right) {
			t.Fatalf("merge order changed the result:\n left  = %+v\n right = %+v", left.State(), right.State())
		}
	})
}
