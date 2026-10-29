---
title: Acknowledgement-aware Await restart evidence — 29 October 2026
---

# Acknowledgement-aware Await restart evidence

This is a bounded local acceptance checkpoint for [issue #1023](https://github.com/The-Pipeline-Framework/pipelineframework/issues/1023)
and [ADR-0079](../decisions/0079-command-completion-can-require-dispatch-acknowledgement.md).
It is partial evidence, not closure of either the framework acceptance gate or the
product's live GitHub/hosting obligations. CSV remained parked; no cloud resources
or spending were used.

## Frozen inputs and fixture

The consumer was based on product-set-verification integration head
`8178eb4585328f74f2d50b40205bac284da45ac6`, merged in stack commit
`784d0a3a0467116ead35b41509491914e4134572`. The local fixture delta adds optional
test signing-key continuity, server-configured manual delivery and explicit Java
bindings for the authored root/fixture types and their sealed-union alternatives.
The random signing-key default and disabled fixture default remain unchanged.

The governed dependency verifier passed against the unchanged published set:

| Component | Published candidate |
| --- | --- |
| Contracts | `26.10.1-main.66cf17d85540` |
| Compiler | `26.10.1-main.d972daa918c6` |
| Runtime | `26.10.1-main.c83c59656ecd` |
| Connectors | `26.10.1-main.455a4f1829c1` |
| CLI/resolver | `26.10.1-main.ce3796f82205` |

Runtime JAR SHA-256:
`1015a2c154517e54471e86aaa33d498e69d8b0518a028274e737152370690a0d`.
Application JAR SHA-256:
`ebce14166c9307eab3eebe8d33d99661d61cc6db036ee7f5c61f91032815885e`.
Local-execution JAR SHA-256:
`4a32d2769d81770ca941540357af91f89e4c6213b887a5326231e31d2388ef9d`.

The application was packaged with the existing Dynamo Command-effect provider;
this is a build-time selection. Runtime execution/Await stores used local
LocalStack 3.8 Dynamo, while the native file release registry retained the actual
application artifact and generated contract. Native admin registration and
activation selected `restart-proof-2`, contract
`sha256:7040017e8bc556b4db11744e41dea9bc9b282b95d7eebe97c3dffe779a7aa363`.
Both JVMs used that same package, registry and test-only authentication identity.
All service endpoints were loopback and AWS credentials were dummy local values.

Six signer tests and five fixture-resource tests passed. The changed application
packaged successfully; the previous 121-test full run predates this fixture delta
and must not be presented as a full regression run of these changes.

## Observed sequence

```mermaid
sequenceDiagram
    participant A as First JVM
    participant D as Local Dynamo
    participant B as Restarted JVM
    participant P as Local fixture provider
    A->>D: Persist trusted acknowledgement and WAITING_EXTERNAL
    Note over A: SIGKILL
    B->>D: Read same execution and pinned release
    P->>B: Signed completion from checked-in test
    B->>D: Complete Await and continue execution
    B-->>P: 202 admitted; duplicate returns 200
    B->>D: Persist SUCCEEDED and final result
    B->>D: Read final result
    Note over B,D: Nested union decoding fails
```

Execution `40db3e86-abe8-4c2e-9978-6389c222cdf7` reached `WAITING_EXTERNAL`
with an acknowledgement retained in Dynamo before the serving JVM was killed.
An external local Java test driver ran the actual checked-in compatible fixture
and delivered its signed result to the restarted application's callback endpoint.
The driver used only operator-owned test secrets; it did not disable authentication
or write completion state directly.

- Completion returned HTTP **202**; its identical duplicate returned **200**.
- The Await record became **COMPLETED** and retained its typed acknowledgement
  and `DeferredFixtureReport` response.
- Native continuation persisted execution **SUCCEEDED**, version **5**, with
  a final `List<ApplicationResult>` payload.
- Canonically sorted Command-effect rows were byte-identical before and after
  restart/completion: no new dispatch or effect revision was observed.
- Result retrieval failed and the browser correctly showed unavailable state;
  it did not display a successful test result.

An earlier setup attempt exceeded the existing 60-second fixture deadline while
the delivery driver was being prepared. It became `TIMED_OUT`/`AWAIT_TIMEOUT`
and rejected the late callback with HTTP 409. That history was retained. The
prepared sequence above ran within the unchanged deadline.

## Stop condition and remaining proof

The recovered runtime reported:

```text
Failed decoding canonical durable payload 'List<ApplicationResult>'
Cannot construct instance of org.productset.app.ApplicationCompletion
```

Inspection of the actual published runtime bytecode confirmed that the durable
plan detects unions at its root runtime class. A list of records takes the normal
Jackson reader/writer path; the nested canonical union does not retain a variant
envelope. The generated contract does contain the `ApplicationCompletion` union
and its `awaitFixtureExecuted` variant, but the stored result contains only the
variant's fields. The failure is final-result materialisation, not loss of the
trusted acknowledgement or failure of its callback join.

The runtime owner must decide recursive canonical union support versus explicit
rejection/documentation of unsupported authored bindings. This proof does not
establish that application Jackson annotations are required or an adequate fix.
No framework repair or annotation workaround was attempted.

The application JVM and dedicated local container were stopped. The packaged
application, source delta, driver, logs, release descriptors and consistent table
exports are retained in the operator's local evidence directory
`/Users/mari/.codex/artifacts/await-restart-proof-20261029`.

Callback-first process restart was not run after this stop condition. Complete
typed result retrieval and both-order acceptance remain open; ADR-0079 stays
draft and #1023 stays open. Live provider delivery and product activation remain
separate, unproved obligations.

## Bounded upstream correction

The runtime correction under review makes sealed-union handling local to each
release-bound JSON plan. Jackson still traverses records and containers, but a
declared sealed union uses the existing `discriminator`, `case`, `value` envelope
at nested positions as well as at the root. Generated union discriminators remain
validated; authored marker unions use their declared case's simple name. Payloads
cannot select an arbitrary class, and canonical type/catalog fingerprints remain
mandatory.

Existing ordinary payloads and root union envelopes remain readable. Old nested
union JSON that omitted variant identity cannot be reconstructed safely by guessing
from its fields. This correction does not repair such stored values or substitute
the current release for their pinned release.

Parameterised sealed-union bases and cases, including erased raw generic bases, are rejected
explicitly. Generic records and containers containing non-generic sealed unions
are covered. The existing canonical presence-value module remains authoritative;
union support inside its custom wrapper is not established by this checkpoint.

The acceptance rerun must use the exact published runtime candidate. For the
callback-first case, a loopback-only JVM debugger breakpoint can stop entry to
`AwaitCoordinator.settleCommandDispatch` after Command success is durable but
before Await acknowledgement settlement. Before killing that JVM, evidence must
show a staged callback, retained successful Command output and a non-terminal
execution without a final result. Native lease recovery must then replay the
recorded acknowledgement without dispatching again and produce a readable result.
This is distinct from restarting an already-completed execution.

No successful rerun is claimed by this correction proposal. Both complete
restart acceptance cases remain gates until their exact candidate evidence is
recorded.

The upstream correction is [runtime PR #76](https://github.com/The-Pipeline-Framework/pipelineframework-runtime/pull/76),
head `e7ca413cf4d5e4839c5842926ffad771ac722fb4`. The full runtime unit suite passed
2,286 tests before two final review guards; all 42 focused codec, resolver and
routing tests passed after those guards. Owner CI, immutable candidate publication
and the two packaged restart cases remain pending.
