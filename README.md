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

e := artel.NewEngine(c, tr)
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

## Roadmap to 1.0

- Simulation & correctness harness: deterministic adversarial-network testing
  as a public feature, not internal plumbing
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
