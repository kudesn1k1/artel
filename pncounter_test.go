package artel

import (
	"reflect"
	"testing"
)

func samePN(a, b PNCounterState) bool { return reflect.DeepEqual(a, b) }

func TestDeltaPNCounter(t *testing.T) {
	// Delta exchange converges, and Value is signed (can go negative).
	t.Run("delta exchange converges, signed", func(t *testing.T) {
		a := NewPNCounter("A")
		b := NewPNCounter("B")
		a.Increment()
		a.Increment()
		b.Decrement()
		b.Decrement()
		b.Decrement()
		b.Decrement()
		b.Decrement()

		dA, dB := a.Delta(), b.Delta()
		a.Merge(dB)
		b.Merge(dA)

		if !samePN(a.State(), b.State()) {
			t.Fatalf("did not converge: A=%v B=%v", a.State(), b.State())
		}
		if a.Value() != -3 || b.Value() != -3 {
			t.Fatalf("want -3/-3 (2 inc - 5 dec), got A=%d B=%d", a.Value(), b.Value())
		}
	})

	// A delta is a state, so re-joining it is idempotent.
	t.Run("delta join is idempotent", func(t *testing.T) {
		src := NewPNCounter("S")
		src.Increment()
		src.Increment()
		src.Decrement()
		d := src.Delta()

		once := NewPNCounter("T")
		once.Merge(d)
		twice := NewPNCounter("T")
		twice.Merge(d)
		twice.Merge(d)
		if !samePN(once.State(), twice.State()) {
			t.Fatalf("not idempotent: once=%v twice=%v", once.State(), twice.State())
		}
	})

	// Successive inc AND dec must both accumulate into one delta-group.
	t.Run("buffer collapses successive inc and dec", func(t *testing.T) {
		a := NewPNCounter("A")
		a.Increment()
		a.Increment()
		a.Increment()
		a.Decrement()

		f := NewPNCounter("F")
		f.Merge(a.Delta())
		if f.Value() != 2 {
			t.Fatalf("want 2 (3 inc - 1 dec), got %d", f.Value())
		}
	})

	// Absolute inc/dec entries let a replica that missed an earlier delta catch up.
	t.Run("absolute delta tolerates a missed earlier delta", func(t *testing.T) {
		a := NewPNCounter("A")
		a.Increment()
		d1 := a.FlushDelta() // grab the first delta AND reset the buffer

		b := NewPNCounter("B")
		b.Merge(d1)

		a.Increment()
		a.Decrement()
		d := a.FlushDelta()

		c := NewPNCounter("C")
		c.Merge(d) // C never saw d1
		if c.Value() != 1 {
			t.Fatalf("absolute delta should carry inc=2,dec=1 → 1, got %d", c.Value())
		}
	})

	// FlushDelta returns the accumulated delta AND empties the buffer, without
	// touching state.
	t.Run("FlushDelta returns the delta, empties buffer, keeps state", func(t *testing.T) {
		a := NewPNCounter("A")
		a.Increment()
		a.Increment()
		a.Decrement()

		flushed := a.FlushDelta()
		if flushed.inc["A"] != 2 || flushed.dec["A"] != 1 {
			t.Fatalf("flushed delta should carry inc=2,dec=1, got inc=%d dec=%d", flushed.inc["A"], flushed.dec["A"])
		}
		if a.Value() != 1 {
			t.Fatalf("flush must not touch state; want 1, got %d", a.Value())
		}

		b := NewPNCounter("B")
		before := b.Value()
		b.Merge(a.Delta()) // buffer empty after flush → no-op
		if b.Value() != before {
			t.Fatalf("empty delta after flush should be a no-op; before=%d after=%d", before, b.Value())
		}
	})

	// Join(S) S must be pure — neither operand may be mutated (both inner maps).
	t.Run("Join is pure — does not mutate operands", func(t *testing.T) {
		a := PNCounterState{inc: map[string]uint64{"X": 1}, dec: map[string]uint64{}}
		b := PNCounterState{inc: map[string]uint64{"X": 5, "Y": 2}, dec: map[string]uint64{"X": 3}}

		_ = a.Join(b)

		if !reflect.DeepEqual(a.inc, map[string]uint64{"X": 1}) || len(a.dec) != 0 {
			t.Fatalf("Join mutated receiver: a.inc=%v a.dec=%v", a.inc, a.dec)
		}
		if !reflect.DeepEqual(b.inc, map[string]uint64{"X": 5, "Y": 2}) || !reflect.DeepEqual(b.dec, map[string]uint64{"X": 3}) {
			t.Fatalf("Join mutated argument: b=%v", b)
		}
	})

	// A grabbed delta stays a stable snapshot across later local mutation.
	t.Run("Delta snapshot is stable across later mutation", func(t *testing.T) {
		a := NewPNCounter("A")
		a.Increment()
		snap := a.Delta()
		a.Increment()
		a.Decrement()
		if snap.inc["A"] != 1 || len(snap.dec) != 0 {
			t.Fatalf("snapshot moved under later mutation: inc=%v dec=%v", snap.inc, snap.dec)
		}
	})
}
