---
title: Polyglot contract qualification precedes runtime selection
status: accepted
---

# ADR-0088: Polyglot contract qualification precedes runtime selection

## Context

Native same-process LOCAL execution is a requirement for the proposed polyglot developer
experience. Streaming, Connectors and durable Await also remain requirements. A unary
experiment can clarify the execution boundary, but cannot define that product's MVP.

[ADR-0080](./0080-native-runtimes-consume-compiler-owned-execution-contracts.md) proposes
language-specific execution runtimes that consume compiler-owned contracts. The first research profile and compatibility choices are settled below;
production runtime selection and deployment qualification remain separate. Native kernels, an embedded-language
host such as Detroit and a Java daemon also have different developer-workflow consequences.
A small successful example is not enough to select between them.

## Decision

Qualify a shared execution contract before selecting or implementing production language
runtimes. Keep the Java compiler as semantic authority and the Java coordinator as durable
authority. Each execution host implements the supported policies under that contract.

```mermaid
flowchart LR
    C[Compiler-owned meaning] --> P[Shared execution contract]
    P --> J[Java execution]
    P --> N[Node execution]
    P --> Y[Python execution]
    J --> Q[Cross-language qualification]
    N --> Q
    Y --> Q
    Q --> D[Production runtime decision]
```

### Declare the supported profile

A versioned profile defines supported canonical types, cardinalities, policies and runtime
arrangements. Unknown versions, required capabilities, logical types or failure variants
must be rejected explicitly. The compiler rejects unsupported authored features; the
runtime rejects incompatible generated artefacts before invoking business functions.

The compiler owns DSL meaning, composition, identity, placement and policy projection.
Contracts owns portable data and protocol meaning. Runtime owns execution and lifecycle.
A language implementation must not independently interpret YAML, infer missing policies
or silently omit a configured operation.

Research profiles may be deliberately small. Their exclusions describe the experiment,
not a permanent ceiling on TPF or a reduction of the intended product requirements.

### Preserve values and identity

Preserve [field presence and nullability](./0016-field-presence-nullability-and-compatibility.md)
and [canonical JSON presence](./0074-canonical-presence-json.md). Omission, explicit null
and a value are distinct. Required and non-null constraints are independent; false, zero
and empty values remain present.

Cross-language representations must preserve numeric precision and define any permitted
normalisation. Wire identity comes from the compiler's canonical model and IDL lock,
not declaration order, Java class names or native type names. Hashes and signatures apply
to their specified bytes; semantic similarity does not make differently encoded bodies
interchangeable. Contract identity, release identity and artefact digests remain distinct.

New payload or structured-failure encodings require explicit compatibility rules and
release-bound support. A failed compatibility check must not trigger a fallback decoder
or turn an unknown failure into a retryable one. Existing Java encodings remain unchanged
until an explicit migration is implemented and qualified.

### Keep the three interfaces separate

| Interface | Responsibility |
|---|---|
| Application API | Typed direct invocation or authenticated durable admission, status, results and completion |
| Step invocation | One business operation under its owning execution policy |
| Transition-worker protocol | An admitted range under pinned identity, returning completion, failure or suspension |

An application API does not grant transition authority. A worker does not re-admit its
execution, select a different release or take ownership of durable recovery.

### Assign every operation an owner

A returns to its execution shell. That shell validates the supported result and follows
the compiler-defined flow before invoking B. Every enabled operation at that boundary
needs a named policy owner, executor, state owner, retry owner and acknowledgement rule.

| Operation | Contract ownership |
|---|---|
| Sequencing, result validation and local attempts | Language execution shell, following the compiler projection and shared runtime policy |
| Remote step binding | Generated caller/callee bindings; the callee owns logical step retries, without an additional caller retry budget |
| Transition delivery, settlement and recovery | Java coordinator; worker completion and durable settlement are separate events |
| Context, cancellation and semantic observations | Shared runtime contract with explicit language bindings and exporter policy |
| Cache and persistence aspects | Existing plugin/runtime owner; declared hooks cannot be silently omitted or substituted for capture storage |
| Query capture and replay | Authorised capability host and capture store; capture commits before success and replay bypasses the provider |
| Command effects | Application effect authority and Command host; ambiguous outcomes require explicit reconciliation |
| Connections | Host tenant/actor authorisation and credential lifecycle; only authorised logical references cross the boundary |
| Await | Java durable authority and persisted continuation data, independent of the live language process |
| Streaming, checkpoints and nested invocation | Their existing semantic owners; each portable binding must preserve its own lifecycle and identity guarantees |

Logical attempts, physical network requests and coordinator redeliveries are different
counts. A lost response may cause repeated execution; there is no exactly-once
business-function invocation promise. Cancellation is not rollback, and a late result
must not start the next operation after cancellation has won. Only explicitly supported
deadlines and budgets may be propagated; this decision introduces no universal timeout.

### Map host behaviour explicitly

The unary research contract uses a separately versioned policy profile. It does not
reinterpret an older probe or change existing Java defaults. A future binding can claim
that profile only when it implements the following mappings and rejects incompatible
configuration before invoking a function.

**Failures and attempts.** Only a validated `TRANSIENT` failure is eligible for logical
step retries, under the admitted owner's remaining budget. `BUSINESS_REJECTION` is a
terminal, explicitly classified outcome; a Java non-retryable exception marker alone
does not establish business rejection. An unrecognised host exception becomes terminal
`UNCLASSIFIED`, with a fixed, sanitised message. Exception class names, stacks and arbitrary
messages do not cross that portable boundary.

A structured failure is different from a host exception. Unknown codes, malformed fields
and conflicts with the admitted step or owner are protocol errors. A claimed `retryable`
flag cannot grant retry authority: it must agree with the known code. Exhausting a budget
preserves the last validated failure; it does not invent a successful zero or null result.
Retrying B reuses B's accepted input and does not rerun A.

There is one logical retry owner. A Java binding with independently active legacy retries
or failure recovery that can alter settlement cannot claim this profile. The binding must reject that
combination before A starts. This restriction does not disable those behaviours in
unprofiled Java applications.

**Results.** A returned value, empty completion and failure have distinct discriminants.
An explicit `VALUE(null)` requires an explicitly nullable result declaration. Raw Java
null, JavaScript undefined or Python None without that carrier is invalid: it cannot
identify the intended outcome. Nullable canonical fields do not themselves establish
support for nullable whole results in the compiler. The latter is a boundary experiment
until separately projected and qualified. `EMPTY` is recognisable but unsupported by this
finite ONE_TO_ONE profile; it must not silently invoke B or become successful null.

**Cancellation.** The first terminal event accepted by the execution shell determines the
recorded outcome. Cancellation accepted before invocation prevents A from starting.
A's intermediate return and a failure with a retry still available are not terminal.
Cancellation accepted after A returns but before B dispatches therefore prevents B.
Cancellation before a pending retry prevents that attempt; a later result or failure
cannot revive execution. Conversely, a final completion or failure accepted first remains
recorded when cancellation arrives later. Cancellation cannot undo an invocation already
started, and promises no rollback.

A recorded outcome, a cancellation observation and delivery to an application subscriber
are separate facts. A cancelled subscription need not receive a terminal value. Absence
of delivery alone is not evidence that cancellation won.

**Context.** The profile requires an explicit per-invocation snapshot containing only
`executionId` and `traceId`. Both are strings containing at least one character other than
ASCII space, tab, carriage return or line feed. They are otherwise preserved exactly;
this is no new canonical identifier format. Missing, malformed or additional fields
reject admission, even if ambient context contains apparently usable values. Retries
retain the snapshot. Handler mutation cannot alter the snapshot seen by later steps or
retries; concurrent invocations use separate snapshots. A production binding must deliberately connect this carrier to its host
context mechanism.

These rules define the adopted unary research contract. Independent consumers can check its examples,
but a model of ordered events does not qualify production concurrency, a wire adapter or
an existing Java executor. Those bindings still need their own checks.

### Preserve authority across process boundaries

Remote admission requires authentication, timestamp and nonce handling, tenant
authorisation and compatible pinned identity before execution. Signature rules belong to
shared contracts; secrets, replay protection and authorisation state belong to the host.
A package version alone does not establish release or capability compatibility.

#### Bind the profile to verified release artefacts

Reuse the existing Release Descriptor and its pipeline, contract, release, artefact
and digest identities. Do not introduce a parallel native-language release manifest.
Starting from a deployment-trusted descriptor or admitted Release, the runtime-owned
release resolver verifies the selected runnable artefact and the descriptor-named
Compiled Truth carrier against their pinned digests. Those may be separate artefacts.
The host qualifies the execution binding and the compiler projection from that carrier
as a combination under the same Release and profile. A matching projection hash alone
does not verify the executable, its dependencies or the release's provenance.

The deployment host owns the binding between an authorised tenant, target and verified
release. A configuration-backed binding is sufficient for a bounded, managed-endpoint
experiment. It is not authority supplied by a business function or an untrusted worker.
A worker advertisement must agree with that binding; advertising a profile name or
listing a capability does not prove its implementation. A new artefact or profile
requires qualification of that exact combination before dispatch.

Keep the worker envelope encoding, application payload encoding, execution profile and
result profile distinct. Support for the outer envelope does not select a decoder for
application values or establish execution-policy compatibility. Each selection must be
explicit and release-bound, with no fallback to a similarly named encoding.

#### Select portable failure results explicitly

Portable transition failures require an explicitly selected, versioned result
representation. Do not encode portable classifications in a legacy Java class name or
message, or reconstruct arbitrary native exceptions at the coordinator. A Java
non-retryable marker cannot distinguish business rejection from an unclassified failure.
Existing Java failure envelopes and unprofiled execution retain their current meaning.

The worker normalises host exceptions under the selected policy before serialisation.
The portable result needs a closed failure classification, compiler-owned logical step
identity and a safe diagnostic. It must distinguish an exhausted local transient failure,
business rejection and an unclassified failure without transmitting native class names,
stacks or arbitrary exception messages. Command effect identity belongs to its separately
qualified effect protocol; it cannot be invented to make a unary business failure fit a
legacy envelope.

Exhaustion ends the worker's local attempts. It neither authorises another physical
request nor gives the worker a coordinator retry budget. The coordinator validates the
result against its dispatch attempt and applies its own admitted recovery policy. A stale,
malformed, unknown or incompatible result cannot settle the execution or create retry
authority. Rejecting a response does not prove that its remote invocation stopped;
uncertain outcomes retain the protection defined by
[ADR-0014](./0014-remote-transition-outcome-uncertainty.md).

Select `tpf-polyglot-failed-result/1` for the first failure-result research binding.
Its closed shape is `{resultProfile, outcome: "FAILED", failure}`; the failure fields are
`code`, `stepId` and `diagnostic`. The logical step must belong to the admitted range.
The fixed mappings are:

| Code | Diagnostic | Meaning |
|---|---|---|
| `TRANSIENT_EXHAUSTED` | `Local retry budget exhausted` | The worker exhausted the local `TRANSIENT` budget; no new retry authority is granted |
| `BUSINESS_REJECTION` | `Business function rejected input` | Explicit terminal business classification |
| `UNCLASSIFIED` | `Unclassified business function failure` | A host failure without an admitted classification |

The released `COMPLETED` envelope shape is deliberately shared by both configurations;
its inner application encoding remains independently pinned.
A failure-only profile does not reinterpret successful payloads. Legacy failures under
this selection, portable failures without it, unknown versions, mixed outcome fields and
unsafe diagnostics reject. The response tag confirms a selection; it never chooses a
decoder or triggers a fallback.

### Select compatibility at deployment, not from a response

For the first remote qualification, use a dedicated, explicitly paired profile-aware
coordinator binding and worker endpoint. Deployment-owned configuration binds the
verified Release, runnable, Compiled Truth carrier, tenant, target, execution profile and
failure-result profile. Record that selection with the dispatch attempt. A configuration
change cannot alter an in-flight attempt's decoder. New configurations apply to newly
admitted attempts only.

Do not add dynamic negotiation or a second release manifest for this binding. A generic
worker protocol version or capability advertisement cannot authorise the new failure
shape. Both peers must explicitly implement the selected binding before deployment;
unmodified Java peers are not compatible merely because they accept the same success
representation. Keep legacy and profile-aware deployments isolated by their trusted
configuration and endpoint association. This research contract does not allocate a new
production outer-protocol version or change the meaning of an existing one.

### Authenticate the response channel and retain attempt authority

Select direct HTTPS to the configured worker endpoint for the first deployed
qualification. Validate the certificate chain and hostname normally. Deployment trust
must associate that endpoint with the verified worker; a valid certificate alone does
not prove an artefact's identity. Keep request HMAC, timestamp and nonce checks. Do not
follow redirects or hide retries, failover or decoder negotiation in the client binding.
An intermediary outside that trusted deployment boundary is unsupported by this choice.

Accept a response only on the original authenticated request-response channel and under
its retained dispatch context: tenant, target, Release and artefact identity, execution,
version, attempt, transition key and admitted range. Response fields cannot replace those
pins. This direct-channel binding does not introduce a separate response HMAC and cannot
be reused as an asynchronous callback or message-delivery contract without another
explicit trust decision.

Pre-dispatch rejection starts no work. A malformed or untrusted active response leaves
the attempt unresolved. If dispatch may have run and the channel ends without a validated
result, preserve `REMOTE_OUTCOME_UNKNOWN` under ADR-0014; do not infer no execution or
permit automatic re-drive. Operator re-drive still requires independent disposition
evidence. A stale or unsolicited reply cannot settle or change the uncertainty state of
a newer attempt. A valid failure settles a worker-result observation, while the Java coordinator
retains its separate durable recovery authority.

Business context must not contain provider credentials, resolved clients or coordinator
stores. An allowlisted context is not a sandbox; deployment isolation remains separate.

Await registration must be durable before acknowledgement. A restarted worker resumes
from canonical continuation data, correlation and the pinned release, including any
item or nested-invocation identity. Promises, coroutines, closures and interpreter handles
are not durable continuations. Lost acknowledgements and duplicate completions must not
change ownership or silently repeat an external effect.

## First qualification profile

Adopt the following bounded contract baseline, not a supported production runtime or a
product MVP. Keep `tpf-polyglot-unary-policy/1`, `tpf-polyglot-transition-admission/1`
and `tpf-polyglot-failed-result/1` separately versioned. Earlier value/identity probes
remain evidence for their declared representations; they do not admit every canonical
value into this execution binding.

| Dimension | Selected baseline | Outside this baseline |
|---|---|---|
| Languages | Java reference, Node and Python consumers of the same contract | Claiming one language proves another; selecting a production kernel or Detroit |
| Platform and execution | COMPUTE, direct finite linear ONE_TO_ONE execution | FUNCTION hosting, branches, nested flows and durable execution qualification |
| Business-step transport | Same-process native LOCAL | General REST/gRPC step placement or mixed-language in-process calls |
| Values | The declared generated unary fixture and its non-null int32 record; explicit type/policy rejection | Treating wider value vectors as a qualified executable codec; nullable whole results or empty completion |
| Worker invocation | One whole-bundle REST worker binding, with admission and settlement modelled separately | Claiming deployed QUEUE_ASYNC, HA or an application-facing REST API from those models |
| Policies | One local retry owner, explicit outcome/context/cancellation rules and pinned failed-result selection | Ambient authority, fallback decoders or independent legacy retries/recovery |
| External I/O and hooks | Explicit rejection when unsupported requirements are declared | Query/Command execution, plugins, provider authoring, checkpoints and durable Await |

This baseline is useful for contract qualification and is deliberately **not an MVP**.
No product MVP is selected by this decision: streaming, Connector guarantees and durable
Await remain requirements for a credible experimental product. Do not substitute a
single read-only hosted Query for that scope. Hosted capability invocation and a native
Provider SPI remain separate, unselected extensions; they require their own host
identity, authorisation, capture/replay and effect-authority qualification.

Remote step invocation is not claimed by this baseline. Its ownership rule remains:
the callee execution shell owns logical retries, the caller has no additional logical
retry budget, and coordinator deliveries are counted separately. That rule must be
qualified by a later placement binding before it can claim support. Application-facing
admission/status/completion APIs remain separate from both step RPC and worker dispatch.

## Qualification and extension

Each claimed language and layout needs shared conformance for its declared profile.
Qualification must distinguish supplied fixtures, generated artefacts, actual execution
and deployed recovery. Substituted boundaries remain explicit limitations.

The intended native developer workflow requires no local JVM or coordinator for pure
LOCAL execution, and no local JVM for changed-project generation. Real QUEUE_ASYNC uses a
remote coordinator and deployed development workers; laptop reachability is optional.
A mandatory local JVM would require an explicit product decision. Remote generation does
not promise offline regeneration.

Semantic extensions require a shared-contract revision, named owners and checks for every
affected language. Passing a declared research profile is a stopping point for that
experiment; it does not authorise importing adjacent provider or coordinator internals.
Streaming, Connector guarantees and durable Await must be qualified before accepting a
credible product MVP.

## Consequences

Acceptance of this qualification contract does not select a production runtime, create a Provider SDK or change existing
Java execution. Existing Spring work remains intact. Production implementation is
unscheduled and depends on acceptance of the relevant contracts.

The [polyglot architecture discussion](../evolve/polyglot-applications-assessment.md)
explains the alternatives. Dated source audits, experiments, results, remaining decisions
and delivery tracking belong to [contracts #52](https://github.com/The-Pipeline-Framework/pipelineframework-contracts/issues/52)
and the [polyglot epic](https://github.com/The-Pipeline-Framework/pipelineframework/issues/1055).
