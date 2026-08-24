---
title: Failure, recovery, and checkpoint handoff ownership
status: accepted
---

# ADR-0012: Failure, recovery, and checkpoint handoff ownership

## Context

Business alternatives, bad stream items, systemic runtime failures, external-effect
ambiguity, and cross-pipeline admission require different recovery semantics.

## Decision

Expected business alternatives are typed values or unions. Supported per-item
recover-and-continue uses item rejection. Systemic execution failure uses runtime retry
and DLQ policy. Framework retry does not blindly wrap an effect boundary when another
attempt could duplicate an effect or destroy ambiguous-success protection. Circuit
admission belongs at eligible framework-managed outbound boundaries, not hidden I/O.

Checkpoint handoff is a cross-pipeline ownership boundary. Before downstream admission,
publication backlog and admission failure remain upstream. After admission, the
downstream pipeline owns execution retry, DLQ, and lifecycle. Retry and redrive preserve
the execution, observation, effect, correlation, or handoff identity appropriate to the
boundary rather than manufacturing a new business act.

An ordinary terminal execution re-drive replays execution and does not authorize a new
external-effect attempt. Retrying a retained `FAILED_RETRYABLE` Command effect requires
an explicit Command-retry admission intent. That intent is durable execution metadata for
the retained failed Command step; `CommandEffectStore` independently authorizes and records the
new attempt under the unchanged logical effect identity.

This decision governs failure contracts in `framework/runtime-core`, recovery and
queue-async coordination in `framework/runtime`, checkpoint connectors, and runtime tests.

## Rationale

Recovery is safe only when the authority and identity being retried are explicit.

## Consequences

- Authored steps do not maintain parallel retry ledgers.
- New control-plane storage prefers immutable records, conditional creation, and
  append-only transitions; mutable legacy stores are not precedent.
- Recovery support must report unsupported semantics rather than silently changing identity.
