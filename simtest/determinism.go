package simtest

import (
	"bytes"
	"strings"
	"testing"
)

// RequireDeterministic runs the scenario twice over fresh nodes and fails the
// test unless both runs produce the same JSONL trace and the same final
// states. It is the harness's own self-check and a subject's cheapest
// property: hidden nondeterminism (map iteration, wall-clock, goroutines)
// shows up here before it shows up as a flaky oracle.
func RequireDeterministic(t testing.TB, s Scenario, sub Subject) {
	t.Helper()
	a, b := Run(s, sub), Run(s, sub)
	var ja, jb bytes.Buffer
	if err := a.Trace.WriteJSONL(&ja, s); err != nil {
		t.Fatalf("simtest: WriteJSONL: %v", err)
	}
	if err := b.Trace.WriteJSONL(&jb, s); err != nil {
		t.Fatalf("simtest: WriteJSONL: %v", err)
	}
	if !bytes.Equal(ja.Bytes(), jb.Bytes()) {
		la, lb := strings.Split(ja.String(), "\n"), strings.Split(jb.String(), "\n")
		for i := 0; i < len(la) || i < len(lb); i++ {
			var x, y string
			if i < len(la) {
				x = la[i]
			}
			if i < len(lb) {
				y = lb[i]
			}
			if x != y {
				t.Fatalf("simtest: scenario seed=%d is not deterministic, traces diverge at line %d:\n run 1: %s\n run 2: %s", s.Seed, i, x, y)
			}
		}
	}
	for i := range a.Final {
		if !bytes.Equal(a.Final[i].State, b.Final[i].State) {
			t.Fatalf("simtest: scenario seed=%d is not deterministic, final state of n%d differs:\n run 1: %s\n run 2: %s", s.Seed, i, a.Final[i].Value, b.Final[i].Value)
		}
	}
}
