# artel

[![ci](https://github.com/kudesn1k1/artel/actions/workflows/ci.yml/badge.svg)](https://github.com/kudesn1k1/artel/actions/workflows/ci.yml)

Delta-state CRDT toolkit and anti-entropy engine for Go: replicated data types
that converge without coordination, and the machinery that ships their changes
between replicas.

**артель** — a self-organized crew of equals working toward a shared result
without a boss. The same idea as a library: full-mesh replicas, no coordinator.

> **Status: pre-1.0.** The API is not stable, the engine runs an interim
> anti-entropy protocol, and wire formats may change between 0.x releases.

## Install

```
go get github.com/kudesn1k1/artel
```

## Example

```go
c := artel.NewGCounter("node-a")
tr := transport.NewHTTP("node-a", "127.0.0.1:8001", map[string]string{
	"node-b": "http://127.0.0.1:8002",
})

e := artel.NewEngine(c, tr, artel.GCounterJSON())
if err := e.Start(ctx, 500*time.Millisecond); err != nil {
	log.Fatal(err)
}
defer e.Stop(context.Background())

c.Increment() // reaches every peer, in any network order, exactly once in effect
```

`cmd/demo` runs a live three-node cluster in three terminals — kill a node,
restart it, watch it catch up.

## Guarantees

Convergence (strong eventual consistency), no lost updates, deterministic
conflict resolution, thread-safety — and an explicit list of what is *not*
promised — live in [docs/guarantees.md](docs/guarantees.md).

## Testing your setup

`simtest` runs a protocol core through a deterministic simulation of an
adversarial network and judges the outcome with oracles. Generate scenarios
from seeds, stress them, and shrink a failure to a minimal one:

```go
profile := simtest.Profile{
	NodesMin: 2, NodesMax: 4, MaxOps: 12, MaxFaults: 4,
	OpGen:      func(r *rand.Rand, _ int) string { return fmt.Sprintf("inc:%d", 1+r.IntN(5)) },
	FaultKinds: []simtest.FaultKind{simtest.FaultDrop, simtest.FaultDelay, simtest.FaultDup, simtest.FaultPartition, simtest.FaultAckLost},
	Interval: 5, Horizon: 40, Settle: 50,
}
oracles := []simtest.Oracle{simtest.Convergence(), simtest.CounterSum()}
if failures := simtest.Stress(1, 1000, profile, subject, oracles...); len(failures) > 0 {
	scenario, result := simtest.Shrink(failures[0].Scenario, subject, oracles...)
	fmt.Println(scenario, result.Violations, result.Trace)
}
```

`subject` builds your nodes: a sans-IO `artel.Core` per replica plus the way
to apply an op and observe the state. To run a live engine under the same
anomalies, wrap its transport: `simtest.Chaos(tr, seed, cfg)`. The package is
experimental until 1.0.

## Roadmap to 1.0

- Simulation & correctness harness — shipped in 0.2 as `simtest`
- Target anti-entropy protocol: delta-intervals with causal consistency
- Causal types: OR-Set, MV-Register, OR-Map, LWW-Register
- Persistence

## Development

```
git config core.hooksPath .hooks   # pre-commit gofmt check
go test ./... -race
```

## License

[MIT](LICENSE)
