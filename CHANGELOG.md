# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-08-20

### Added

- Root package `artel`: replica contracts (`StateReplica`, `DeltaReplica`,
  `DeltaState`), the anti-entropy engine with a river-style lifecycle
  (`Start(ctx)` / `Stop(ctx)` / `Stopped()`), and the transport SPI
  (`Transport`, `Message`, `Handler`, `Kind`).
- Delta-state counters: `GCounter`, `PNCounter`.
- `transport` package: an HTTP transport for real networks and a synchronous
  in-process transport for deterministic tests.
- An engine-over-HTTP seam test; package documentation and examples, including
  a type-inference guardrail.
- Project hygiene: MIT license, this changelog, CI (gofmt, vet, build, race
  tests), a pre-commit gofmt hook, a guarantees document
  (`docs/guarantees.md`), README.
- `cmd/demo`: a live three-node cluster demo.

### Changed

- Module renamed from `crdtlab` to `github.com/kudesn1k1/artel`; packages
  flattened into the root.

### Removed

- The state-based playground (8 CRDT types, the v0 instantaneous simulator,
  HLC, VersionVector) — preserved under the `playground` tag.
