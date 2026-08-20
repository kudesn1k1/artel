# demo

Three replicas of a delta-state G-Counter gossiping over HTTP, each rendering its
own state live in the terminal.

## Run

Three terminals, one command each:

```
go run ./cmd/demo --id A --gossip 127.0.0.1:8001 --api 127.0.0.1:9001 --peer B=http://127.0.0.1:8002 --peer C=http://127.0.0.1:8003 --inc-every 1s
go run ./cmd/demo --id B --gossip 127.0.0.1:8002 --api 127.0.0.1:9002 --peer A=http://127.0.0.1:8001 --peer C=http://127.0.0.1:8003 --inc-every 1s
go run ./cmd/demo --id C --gossip 127.0.0.1:8003 --api 127.0.0.1:9003 --peer A=http://127.0.0.1:8001 --peer B=http://127.0.0.1:8002 --inc-every 1s
```

```
crdtlab  node A  replica A#fa753ca5
gossip 127.0.0.1:8001   api 127.0.0.1:9001

  value  26

▸ A#fa753ca5      10  ████████████████████████████
  B#3da10959       8  ██████████████████████
  C#4a4be644       8  ██████████████████████

  peers  B up   C up
```

`--inc-every` is optional; without it a replica only moves when you ask it to:

```
curl -X POST "http://127.0.0.1:9001/inc?by=5"
curl http://127.0.0.1:9002/          # value + the raw per-replica state
```

## What to look at

**Convergence.** Increment on one node; every terminal reaches the same total.
The per-replica rows show *where* each contribution came from — the sum is
derived, the map is the actual CRDT state.

**Partition.** Ctrl-C one node. The survivors mark it `down` and keep converging
with each other. Increment while it is away, bring it back, and it catches up by
pulling full state from a peer.

**Incarnations.** A restarted node reappears under a *new* replica id, and its
previous incarnation stays in everyone's breakdown with the value it had. The
node id (`A`) is the address-book name and is stable; the replica id
(`A#fa753ca5`) is the key this process writes into the lattice and is fresh on
every boot. Reusing it would be unsafe: the join takes the maximum per key, so an
increment made before the catch-up landed would be silently eaten rather than
merged.

## Flags

| flag | default | |
|---|---|---|
| `--id` | — | node id, the stable address-book name (required) |
| `--gossip` | `127.0.0.1:8001` | listen address for gossip |
| `--api` | `127.0.0.1:9001` | listen address for the control API |
| `--peer` | — | `ID=URL`, repeated once per peer |
| `--interval` | `500ms` | gossip round interval |
| `--inc-every` | off | increment on a timer |
| `--refresh` | `100ms` | dashboard refresh |
| `--plain` | off | append frames instead of redrawing; no colour |
| `--verbose` | off | engine logs to stderr |

The dashboard redraws in place with two ANSI sequences and no dependencies. Logs
go to stderr so they do not fight the frame — redirect it if you want a clean
terminal, or use `--plain`.
