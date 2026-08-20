// Package artel is a delta-state CRDT toolkit and anti-entropy engine for Go:
// replicated data types that converge without coordination, and the machinery
// that ships their changes between replicas.
//
// # Types
//
// Every type is a delta-state CRDT: mutations apply locally with no
// round-trips, and each replica accumulates a small delta that the engine
// gossips instead of the whole state. Available today: [GCounter], [PNCounter].
// The roster grows toward registers, sets and a map on the way to 1.0.
//
// # Engine
//
// [Engine] runs anti-entropy gossip for one replica over a pluggable
// [Transport]:
//
//	c := artel.NewGCounter("node-a")
//	e := artel.NewEngine(c, tr)
//	if err := e.Start(ctx, 500*time.Millisecond); err != nil {
//		// ...
//	}
//	defer e.Stop(context.Background())
//
// The transport subpackage provides an HTTP implementation for the real
// network and an in-process one for deterministic tests; anything satisfying
// [Transport] plugs in.
//
// # Status
//
// Pre-1.0: the API is not stable yet, and the engine currently runs an interim
// anti-entropy protocol. docs/guarantees.md spells out what is and is not
// promised today.
package artel
