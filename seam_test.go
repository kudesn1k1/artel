package artel_test

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/kudesn1k1/artel"
	"github.com/kudesn1k1/artel/transport"
)

// The engine and the HTTP transport are each covered alone; this file covers
// the seam between them over real sockets. The riskiest path it pins down:
// answering a Pull does outbound I/O inside an inbound HTTP handler.

// freeAddr reserves a loopback port and hands it back, so parallel runs and
// busy machines do not collide on a hardcoded one.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving a port: %v", err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func TestEngineOverHTTP(t *testing.T) {
	addrs := map[string]string{"A": freeAddr(t), "B": freeAddr(t), "C": freeAddr(t)}
	peersOf := func(self string) map[string]string {
		peers := make(map[string]string, len(addrs)-1)
		for id, addr := range addrs {
			if id != self {
				peers[id] = "http://" + addr
			}
		}
		return peers
	}

	replicas := make(map[string]*artel.GCounter, len(addrs))
	engines := make(map[string]*gEngine, len(addrs))
	for id := range addrs {
		rep := artel.NewGCounter(id + "#1")
		e := artel.NewEngine(rep, transport.NewHTTP(id, addrs[id], peersOf(id)))
		t.Cleanup(func() { _ = e.Stop(context.Background()) })
		if err := e.Start(context.Background(), tick); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
		replicas[id], engines[id] = rep, e
	}

	replicas["A"].IncrementBy(1)
	replicas["B"].IncrementBy(2)
	replicas["C"].IncrementBy(3)

	waitFor(t, "all three replicas to reach 6 over HTTP", func() bool {
		return replicas["A"].Value() == 6 && replicas["B"].Value() == 6 && replicas["C"].Value() == 6
	}, func() string {
		return fmt.Sprintf("A=%d B=%d C=%d", replicas["A"].Value(), replicas["B"].Value(), replicas["C"].Value())
	})

	// Restart C as a new incarnation on the same address. Its increment lands
	// BEFORE Start, so the Pull catch-up gets a deterministic window in which
	// it could swallow the local update — the value must end at 7, with the
	// old incarnation's contribution intact.
	if err := engines["C"].Stop(context.Background()); err != nil {
		t.Fatalf("stop C: %v", err)
	}

	c2 := artel.NewGCounter("C#2")
	c2.IncrementBy(1)
	e2 := artel.NewEngine(c2, transport.NewHTTP("C", addrs["C"], peersOf("C")))
	t.Cleanup(func() { _ = e2.Stop(context.Background()) })
	if err := e2.Start(context.Background(), tick); err != nil {
		t.Fatalf("restart C: %v", err)
	}

	waitFor(t, "the new incarnation to catch up without losing its own update", func() bool {
		return replicas["A"].Value() == 7 && replicas["B"].Value() == 7 && c2.Value() == 7
	}, func() string {
		return fmt.Sprintf("A=%d B=%d C2=%d", replicas["A"].Value(), replicas["B"].Value(), c2.Value())
	})
}
