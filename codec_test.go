package artel

import (
	"bytes"
	"testing"
)

// A codec must be deterministic: equal states, equal bytes. The wire forms
// are ordered and hold no maps, so any codec that writes them in order is.

func encode[S any](t *testing.T, c Codec[S], s S) []byte {
	t.Helper()
	b, err := c.Encode(s)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return b
}

func decode[S any](t *testing.T, c Codec[S], b []byte) S {
	t.Helper()
	s, err := c.Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	return s
}

func requireSameBytes(t *testing.T, what string, got, want []byte) {
	t.Helper()
	if !bytes.Equal(got, want) {
		t.Fatalf("%s:\n got  %s\n want %s", what, got, want)
	}
}

func TestGCounterJSON(t *testing.T) {
	codec := GCounterJSON()

	t.Run("round-trips a state", func(t *testing.T) {
		a := NewGCounter("A")
		a.Increment()
		a.Increment()
		b := NewGCounter("B")
		b.Increment()
		a.Merge(b.State())
		want := a.State()

		got := decode(t, codec, encode(t, codec, want))
		if !sameState(got, want) {
			t.Fatalf("round trip changed the state: got %v, want %v", got, want)
		}
	})

	t.Run("a decoded delta merges across the wire", func(t *testing.T) {
		src := NewGCounter("A")
		src.Increment()
		src.Increment()
		src.Increment()

		dst := NewGCounter("B")
		dst.Merge(decode(t, codec, encode(t, codec, src.FlushDelta())))
		if dst.Value() != 3 {
			t.Fatalf("value did not cross the wire: want 3, got %d", dst.Value())
		}
	})

	// Bottom is bottom whichever path built it: a fresh state, a drained
	// buffer, a decoded encoding of either — all the same bytes.
	t.Run("bottom encodes the same everywhere", func(t *testing.T) {
		fresh := NewGCounter("A")
		var zero GCounterState
		paths := map[string]GCounterState{
			"zero value":     zero,
			"fresh State":    fresh.State(),
			"fresh Delta":    fresh.Delta(),
			"drained buffer": fresh.FlushDelta(),
			"decoded zero":   decode(t, codec, encode(t, codec, zero)),
		}
		want := encode(t, codec, zero)
		for name, s := range paths {
			if !s.IsBottom() {
				t.Fatalf("%s is not bottom", name)
			}
			requireSameBytes(t, name, encode(t, codec, s), want)
		}
	})

	t.Run("equal states encode to equal bytes", func(t *testing.T) {
		a, b, c := NewGCounter("A"), NewGCounter("B"), NewGCounter("C")
		a.Increment()
		b.IncrementBy(2)
		c.IncrementBy(3)

		x := a.State().Join(b.State()).Join(c.State())
		y := c.State().Join(b.State()).Join(a.State())
		requireSameBytes(t, "joined in two orders", encode(t, codec, x), encode(t, codec, y))

		first := encode(t, codec, x)
		for range 20 {
			requireSameBytes(t, "encoded again", encode(t, codec, x), first)
		}
	})

	t.Run("garbage is an error", func(t *testing.T) {
		if _, err := codec.Decode([]byte("not a state")); err == nil {
			t.Fatal("garbage decoded without an error")
		}
	})
}

// One count per replica, ordered by replica id, nothing at bottom.
func TestGCounterWire(t *testing.T) {
	a, b := NewGCounter("b"), NewGCounter("a")
	a.IncrementBy(2)
	b.IncrementBy(5)
	a.Merge(b.State())

	got := a.State().Wire()
	want := GCounterWire{Counts: []ReplicaCount{{"a", 5}, {"b", 2}}}
	if len(got.Counts) != 2 || got.Counts[0] != want.Counts[0] || got.Counts[1] != want.Counts[1] {
		t.Fatalf("wire form is %+v, want %+v", got, want)
	}

	back := GCounterStateFromWire(got)
	if !sameState(back, a.State()) {
		t.Fatalf("FromWire(Wire()) changed the state: got %v, want %v", back, a.State())
	}

	if w := NewGCounter("a").State().Wire(); w.Counts != nil {
		t.Fatalf("bottom has a wire form with counts: %+v", w)
	}
	if !GCounterStateFromWire(GCounterWire{}).IsBottom() {
		t.Fatal("the zero wire form is not bottom")
	}
}

func TestPNCounterJSON(t *testing.T) {
	codec := PNCounterJSON()

	t.Run("round-trips a state", func(t *testing.T) {
		a := NewPNCounter("A")
		a.Increment()
		a.Increment()
		a.Decrement()
		b := NewPNCounter("B")
		b.Decrement()
		a.Merge(b.State())
		want := a.State()

		got := decode(t, codec, encode(t, codec, want))
		if !samePN(got, want) {
			t.Fatalf("round trip changed the state: got %v, want %v", got, want)
		}
	})

	t.Run("a decoded delta merges across the wire", func(t *testing.T) {
		src := NewPNCounter("A")
		src.Increment()
		src.Increment()
		src.Decrement()

		dst := NewPNCounter("B")
		dst.Merge(decode(t, codec, encode(t, codec, src.FlushDelta())))
		if dst.Value() != 1 {
			t.Fatalf("value did not cross the wire: want 1, got %d", dst.Value())
		}
	})

	t.Run("bottom encodes the same everywhere", func(t *testing.T) {
		fresh := NewPNCounter("A")
		var zero PNCounterState
		paths := map[string]PNCounterState{
			"zero value":     zero,
			"fresh State":    fresh.State(),
			"fresh Delta":    fresh.Delta(),
			"drained buffer": fresh.FlushDelta(),
			"decoded zero":   decode(t, codec, encode(t, codec, zero)),
		}
		want := encode(t, codec, zero)
		for name, s := range paths {
			if !s.IsBottom() {
				t.Fatalf("%s is not bottom", name)
			}
			requireSameBytes(t, name, encode(t, codec, s), want)
		}
	})

	t.Run("equal states encode to equal bytes", func(t *testing.T) {
		a, b := NewPNCounter("A"), NewPNCounter("B")
		a.Increment()
		a.Decrement()
		b.IncrementBy(2)

		x := a.State().Join(b.State())
		y := b.State().Join(a.State())
		requireSameBytes(t, "joined in two orders", encode(t, codec, x), encode(t, codec, y))
	})

	t.Run("garbage is an error", func(t *testing.T) {
		if _, err := codec.Decode([]byte("not a state")); err == nil {
			t.Fatal("garbage decoded without an error")
		}
	})
}

func TestPNCounterWire(t *testing.T) {
	a, b := NewPNCounter("b"), NewPNCounter("a")
	a.IncrementBy(2)
	a.Decrement()
	b.IncrementBy(5)
	a.Merge(b.State())

	got := a.State().Wire()
	want := PNCounterWire{
		Inc: []ReplicaCount{{"a", 5}, {"b", 2}},
		Dec: []ReplicaCount{{"b", 1}},
	}
	if len(got.Inc) != 2 || got.Inc[0] != want.Inc[0] || got.Inc[1] != want.Inc[1] ||
		len(got.Dec) != 1 || got.Dec[0] != want.Dec[0] {
		t.Fatalf("wire form is %+v, want %+v", got, want)
	}

	back := PNCounterStateFromWire(got)
	if !samePN(back, a.State()) {
		t.Fatalf("FromWire(Wire()) changed the state: got %v, want %v", back, a.State())
	}

	if w := NewPNCounter("a").State().Wire(); w.Inc != nil || w.Dec != nil {
		t.Fatalf("bottom has a wire form with counts: %+v", w)
	}
	if !PNCounterStateFromWire(PNCounterWire{}).IsBottom() {
		t.Fatal("the zero wire form is not bottom")
	}
}

func TestJSONCodecOverAUserType(t *testing.T) {
	type flag struct{ on bool }
	type flagWire struct {
		On bool `json:"on"`
	}
	codec := JSONCodec(
		func(f flag) flagWire { return flagWire{On: f.on} },
		func(w flagWire) flag { return flag{on: w.On} },
	)
	requireSameBytes(t, "encoded flag", encode(t, codec, flag{on: true}), []byte(`{"on":true}`))
	if got := decode(t, codec, []byte(`{"on":true}`)); !got.on {
		t.Fatalf("decoded %+v, want on", got)
	}
}
