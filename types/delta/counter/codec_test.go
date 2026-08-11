package counter

import (
	"testing"
)

// RED spec for the wire codec (design §4). Each *State* type serializes ITSELF —
// the method lives in the type's package and legitimately touches its own private
// maps, so the engine can stay generic over []byte. Implement, per type:
//
//	func (s GCounterState) MarshalBinary() ([]byte, error)
//	func (s *GCounterState) UnmarshalBinary(b []byte) error
//
// json-of-the-map is fine for MVP (readable in the demo logs); the format is
// yours to make compact later without touching the engine.

func TestGCounterStateCodec(t *testing.T) {
	// A non-trivial state (two replicas' entries) survives marshal→unmarshal byte-for-byte.
	t.Run("round-trips a state", func(t *testing.T) {
		a := NewGCounter("A")
		a.Increment()
		a.Increment()
		b := NewGCounter("B")
		b.Increment()
		a.Merge(b.State()) // a.state = {A:2, B:1}
		want := a.State()

		blob, err := want.MarshalBinary()
		if err != nil {
			t.Fatalf("MarshalBinary: %v", err)
		}
		var got GCounterState
		if err := got.UnmarshalBinary(blob); err != nil {
			t.Fatalf("UnmarshalBinary: %v", err)
		}
		if !sameState(got, want) {
			t.Fatalf("round-trip changed the state: got %v, want %v", got, want)
		}
	})

	// The real wire path: flush a delta, ship the bytes, decode on another node,
	// merge — the value must cross.
	t.Run("a decoded delta merges across the wire", func(t *testing.T) {
		src := NewGCounter("A")
		src.Increment()
		src.Increment()
		src.Increment()

		blob, err := src.FlushDelta().MarshalBinary()
		if err != nil {
			t.Fatalf("MarshalBinary: %v", err)
		}
		var wire GCounterState
		if err := wire.UnmarshalBinary(blob); err != nil {
			t.Fatalf("UnmarshalBinary: %v", err)
		}

		dst := NewGCounter("B")
		dst.Merge(wire)
		if dst.Value() != 3 {
			t.Fatalf("value did not cross the wire: want 3, got %d", dst.Value())
		}
	})

	// Empty state must round-trip cleanly (the nil-map / "null" vs "{}" trap).
	t.Run("empty state round-trips", func(t *testing.T) {
		want := NewGCounter("A").State()

		blob, err := want.MarshalBinary()
		if err != nil {
			t.Fatalf("MarshalBinary: %v", err)
		}
		var got GCounterState
		if err := got.UnmarshalBinary(blob); err != nil {
			t.Fatalf("UnmarshalBinary: %v", err)
		}
		if !sameState(got, want) {
			t.Fatalf("empty round-trip changed the state: got %v, want %v", got, want)
		}
	})
}

func TestPNCounterStateCodec(t *testing.T) {
	// Both inc and dec maps must survive the round-trip.
	t.Run("round-trips a state", func(t *testing.T) {
		a := NewPNCounter("A")
		a.Increment()
		a.Increment()
		a.Decrement()
		b := NewPNCounter("B")
		b.Decrement()
		a.Merge(b.State()) // inc{A:2}, dec{A:1, B:1}
		want := a.State()

		blob, err := want.MarshalBinary()
		if err != nil {
			t.Fatalf("MarshalBinary: %v", err)
		}
		var got PNCounterState
		if err := got.UnmarshalBinary(blob); err != nil {
			t.Fatalf("UnmarshalBinary: %v", err)
		}
		if !samePN(got, want) {
			t.Fatalf("round-trip changed the state: got %v, want %v", got, want)
		}
	})

	t.Run("a decoded delta merges across the wire", func(t *testing.T) {
		src := NewPNCounter("A")
		src.Increment()
		src.Increment()
		src.Decrement() // value 1

		blob, err := src.FlushDelta().MarshalBinary()
		if err != nil {
			t.Fatalf("MarshalBinary: %v", err)
		}
		var wire PNCounterState
		if err := wire.UnmarshalBinary(blob); err != nil {
			t.Fatalf("UnmarshalBinary: %v", err)
		}

		dst := NewPNCounter("B")
		dst.Merge(wire)
		if dst.Value() != 1 {
			t.Fatalf("value did not cross the wire: want 1, got %d", dst.Value())
		}
	})

	t.Run("empty state round-trips", func(t *testing.T) {
		want := NewPNCounter("A").State()

		blob, err := want.MarshalBinary()
		if err != nil {
			t.Fatalf("MarshalBinary: %v", err)
		}
		var got PNCounterState
		if err := got.UnmarshalBinary(blob); err != nil {
			t.Fatalf("UnmarshalBinary: %v", err)
		}
		if !samePN(got, want) {
			t.Fatalf("empty round-trip changed the state: got %v, want %v", got, want)
		}
	})
}
