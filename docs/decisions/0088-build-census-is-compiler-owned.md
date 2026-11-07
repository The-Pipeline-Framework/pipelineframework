---
title: Build census belongs to the compiler boundary
status: accepted
---

# ADR-0088: Build census belongs to the compiler boundary

## Context

Ecosystem statistics need evidence of applications developers compile. Runtime metrics,
release identity, Maven dependencies, and generated execution metadata answer different
questions and can expose application details.

## Decision

TPF ecosystem telemetry observes what developers build, never what their applications
process. The compiler owns a pure, allowlisted projection of successfully linked logical
applications, before module filtering can change their apparent size. Its build host owns
an isolated daemon shell for identity, local deduplication, and HTTPS JSON delivery.
The production phase sequence is shared; Maven is a frontend, not the observation owner.
JSR-269 reports only the application authority that generates the orchestrator, after the
final error-free processing round. Service, plugin, Block, and partial-module builds do
not independently report. Release-consuming CLI and runtime adapters do not report.

Random retained installation and application-instance IDs identify reporting instances.
Source paths are keyed locally with HMAC; structural/report digests remain local. Shape
or version changes update an instance rather than minting an application identity.
Only coarse counts, known enums, normalized component versions, Java major, IDs, and a
UTC date cross the boundary. Runtime data, names, coordinates, schemas, endpoints, and
application fingerprints never cross it.

The separately owned census collector accepts a strict versioned protocol and retains
private observations and period membership for a bounded time. Public statistics are
fixed, suppressed, identifier-free aggregates. Schema changes require a reviewed new
protocol version; runtime OTel configuration cannot expand collection.

## Rationale

A compiler-owned observation makes build frontends consistent and preserves the logical
application through deployment layouts. Local fingerprints prevent accidental global
project tracking. A dedicated JSON protocol is smaller and more inspectable than adding
an observability SDK to compilation.

## Consequences

- Reporting cannot fail, wait for networking, or keep the compiler JVM alive.
- Opt-out wins across environment, JVM, and compiler options and creates no census state.
- Disposable builders require an explicit declaration of retained census state.
- Counts describe reporting application instances, not globally unique projects or builds.
- Persistent IDs are pseudonymous; default-on release requires legal and operational approval.
- Preview can ship independently of public reporting. Current support and release gates live
  in the compilation guidance and internal release checklist.

This decision governs `pipelineframework-compiler`, component version resources in
`pipelineframework-contracts`, the separate `pipelineframework-census` collector, and
compiler census documentation. Runtime telemetry remains governed by ADR-0011.
