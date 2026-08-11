package counter

import (
	"reflect"
	"testing"

	"crdtlab/crdt"
)

func sameGC(a, b *GCounter) bool { return reflect.DeepEqual(a.State(), b.State()) }

func TestGCounter(t *testing.T) {
	t.Run("converges under lopsided sync", func(t *testing.T) {
		net := crdt.NewNetwork[*GCounter, GCounterState]()
		net.Add("A", NewGCounter("A"))
		net.Add("B", NewGCounter("B"))
		net.Add("C", NewGCounter("C"))

		net.Get("A").Increment()
		net.Get("A").Increment()
		net.Get("B").Increment()
		net.Get("C").Increment()
		net.Get("C").Increment()
		net.Get("C").Increment()

		net.Sync("A", "B")
		net.Sync("C", "B")
		net.SyncAll()

		if !net.Converged() {
			t.Fatal("replicas did not converge")
		}
		for _, id := range []string{"A", "B", "C"} {
			if got := net.Get(id).Value(); got != 6 {
				t.Errorf("replica %s: Value() = %d, want 6", id, got)
			}
		}
	})

	t.Run("merge is idempotent (duplicate delivery)", func(t *testing.T) {
		src := NewGCounter("S")
		src.IncrementBy(5)

		once := NewGCounter("T")
		once.Merge(src.State())
		twice := NewGCounter("T")
		twice.Merge(src.State())
		twice.Merge(src.State()) // a re-delivered duplicate must change nothing
		if !sameGC(once, twice) {
			t.Fatalf("idempotence: once=%v twice=%v", once.State(), twice.State())
		}
	})

	t.Run("merge is commutative and associative", func(t *testing.T) {
		x := NewGCounter("X")
		x.IncrementBy(2)
		y := NewGCounter("Y")
		y.IncrementBy(1)
		z := NewGCounter("Z")
		z.IncrementBy(3)

		left := NewGCounter("L")
		left.Merge(x.State())
		left.Merge(y.State())
		left.Merge(z.State())
		right := NewGCounter("R")
		right.Merge(z.State())
		right.Merge(y.State())
		right.Merge(x.State())

		if !sameGC(left, right) {
			t.Fatalf("merge order changed result: left=%v right=%v", left.State(), right.State())
		}
		if left.Value() != 6 {
			t.Fatalf("want 6, got %d", left.Value())
		}
	})
}
