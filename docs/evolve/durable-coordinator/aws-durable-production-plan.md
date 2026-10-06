# AWS Durable production completion plan

The AWS Durable architecture is settled by ADR-0069. Complete the supported host already merged in runtime PR #34; do not extract another host or revisit semantic decomposition. The bounded, idempotent, recoverable action prerequisite is established. Durable facts must be replayed through inspection, reconciliation of missing consequences, and checkpoint convergence before work is retired.

## Baseline and audit result

Audit baseline, 6 October 2026:

- Documentation: `pipelineframework` main `b232cadb65fadae25a54f65ae17dea4eb039135c` (#998).
- Runtime: main `28dbeef9fed0f42b72384c27ad95f6ba2fc2b0c4` (#34), plus prerequisite PR #37 head `c18cd26805aeb0edbb9853c02c781891f9839720` in an isolated worktree. #37 remains open at audit time; its inclusion does not imply it is merged.
- Compiler: main `c5c29dab1b1c9238e56cfc2cc743765a4b8eb860`.
- GitNexus used the explicit remote-main indexes, then current source was verified. Runtime graph does not include #37; it is not evidence for that prerequisite. No index refresh or cloud deployment is required for this audit.

The production artifact shape and generic handlers exist. Production support still requires closure of these source-confirmed gaps:

| Gap | Current source | Consequence |
| --- | --- | --- |
| Internal semantic schema parsing | `AwsDurableAwaitStreamProcessor.processAwait` reads five Await table attributes | Host couples to an internal storage representation despite the semantic read seam |
| Generation admission | `register` and `bind` condition only the individual row; `wake` checks latest interaction binding separately | An older registration can bind before the newer registration has produced an interaction binding; read/write races need an atomic generation protocol |
| Repeated Await | Driver names callbacks `await-completion-N`, registration key is only execution/generation | A second callback in one generation conflicts with the first; multi-Await support is not proven by terminal-Await evidence |
| Targeted lookup | Provider ARN GSI includes both registration and binding records; query limits evaluation to one before Java filtering | A binding can hide the registration needed for repair |
| Repair event retirement | `AwsDurableTerminalReconciler` returns false when the eventually consistent GSI is empty | A successful invocation can retire an event before the registration is visible |
| Reconstruction coverage | Terminal reconciler only repairs `WAITING_EXTERNAL`; registration begins at callback creation | A provider loss before first registration, lost mechanical rows, and non-Await nonterminal execution need a retained recovery locator |
| Action version pinning | Durable version invokes unqualified action function; replacement starter uses live alias | Parked and replacement executions may call incompatible action code after deployment |
| Suspended work timing | Driver sweeps while polling, then suspends on callback; production reconciler only handles provider terminal events | Hosting must explicitly drive bounded timeout/continuation/retry consequences while suspended; proof scheduled sweeping cannot silently supply this |
| Operational template | Version has no retention policies; visibility equals 900-second worker timeout; Stream destination lacks send permission | Deployment can delete parked versions or exhaust delivery retries without adequate retry headroom |
| Evidence scope | Proof driver/model/repository reuse production, but proof actions, wake-up, resolver and scheduled reconciler still supply behavior | The 22 historical scenarios establish architecture, not complete production-artifact conformance |
| Platform selection | Compiler rejects AWS_DURABLE unless the coordinator package is FUNCTION | COMPUTE worker protocol reuse is possible; a generated all-COMPUTE application is not currently proven |

## 1 Production modules and packages

Keep the existing runtime-owned reactor. No new repository or multi-cloud SPI is needed.

| Published coordinate, group `org.pipelineframework` | Runtime path | Responsibility |
| --- | --- | --- |
| `pipelineframework-aws-durable-coordination-parent` | `aws-durable-coordination/pom.xml` | Version-aligned parent and dependency management |
| `pipelineframework-aws-durable-coordination-model` | `aws-durable-coordination/model` | Immutable provider messages, driver checkpoint, mechanical identities, naming; package `org.pipelineframework.aws.durable.model` |
| `pipelineframework-aws-durable-coordination-host` | `aws-durable-coordination/host` | Durable driver, action invocation, registration/binding, history classification, replacement, targeted repair, Lambda/SQS adapters; package `org.pipelineframework.aws.durable` |
| `pipelineframework-aws-durable-coordination-deployment` | `aws-durable-coordination/deployment` | Versioned SAM resource `aws-durable-queue-async.yaml`, deployment contract validation |

Existing `pipelineframework` runtime owns semantic action implementations and Dynamo semantic codecs/adapters. Existing contracts repository owns provider-neutral contracts. Compiler owns application-specific generation. Proof modules remain non-published. Keep the current optional application-runtime dependency on the host; the standalone plain Java coordinator must package its runtime dependency closure without requiring a Quarkus action host.

Validate all four coordinates in candidate collection, trusted publication, product BOM and compatibility-set closure. Merely adding them to the reactor does not establish publication or application availability. Freeze the supported release only after the exact set passes conformance.

## 2 Proof code disposition

| Proof surface | Disposition |
| --- | --- |
| Shared messages and Durable driver | Already reuse production model/driver; retain fixture entry points only |
| `ProofControlPlaneActionAdapter` | Rewrite as a fault-decorated invocation of `AwsDurableControlPlaneActions`; retain fixture input/release setup outside the delegate |
| `ProofCallbackBindingRepository` | Production delegate retained; remove scan-based reconciliation from the eventual conformance execution path |
| `ProofDurableHostActionAdapter`, wake-up, callback client, starter | Rewrite as decorators over production services; fault injection wraps real boundaries |
| `ProofDurableBindingResolver`, recovery service, scheduled reconciler | Retain historical diagnostic fixtures until targeted production reconstruction covers every scenario; then delete duplicate algorithms |
| Hard-coded Await descriptors, order/contract JSON, release catalogue | Retain only as historical fixture until a compiler-built conformance application supplies actual release artifacts; never publish in host |
| Fault injector/table, seeded races, provider-history unavailability injection | Retain as isolated conformance instrumentation, never in production artifact dependency closure |
| 22-scenario catalogue, deployed tests, version-pinning and race evidence | Retain stable IDs and assertions; migrate scenario by scenario to production artifacts |
| Raw semantic-table parsing | Move storage interpretation to its runtime semantic-store owner; remove from AWS host and normal conformance execution |

Do not destroy valuable historical evidence while migrating. Archive its source pins and reports; a passing scenario using a legacy repair substitute must not satisfy the production support gate.

## 3 Callback storage and API contract

TPF completion remains the sole semantic authority. AWS registration, binding, delivery receipts and provider execution locators are disposable mechanical records in a separate table. No provider callback identifier enters `AwaitSemanticCheckpoint` or TPF Await records.

Retain immutable conditional writes and append-only records. Do not introduce `UpdateItem` or an upsert-based head. Extend the existing repository with transactional admission of immutable generation claims: generation one claims absent predecessor; replacement generation G+1 consumes exactly one immutable successor claim for G, and all registration/binding admissions condition on the claimed generation. Keep generation-fence evidence beyond callback TTL for the supported recovery lifetime; TTL deletion cannot re-authorise a stale generation.

Represent callback occurrence separately from provider generation. An execution can have multiple Await suspensions within one generation. Proposed registration key: execution scope + zero-padded generation + callback occurrence; binding key: interaction scope + generation + occurrence. Preserve existing generation-one/first-occurrence readers during a documented table migration, then remove transitional readers after retained executions drain. Include a schema version in new mechanical records and retain tenant-scoped, unambiguous identity encoding.

Repository contract:

- Claim successor generation conditionally and return explicit created/replayed/stale/conflicting outcomes.
- Register callback only for the admitted generation/occurrence. Equal semantic checkpoint and provider callback authority replay successfully regardless of housekeeping timestamps. Conflicting callback authority fails closed.
- Bind only against the matching registration and semantic identity observed through the control plane. Reject stale generation admissions atomically; latest-binding queries alone are not a fence.
- Record delivery evidence idempotently after provider acceptance or confirmed success in public history. A delivery receipt is mechanical evidence, never proof that TPF continuation work is complete.
- Query targeted registrations by provider ARN. Index lag or bounded-page exhaustion yields retry; it must not silently retire repair work.
- Distinguish OPEN, SUCCEEDED, CLOSED/missing history and UNKNOWN. Throttling, access denial, transient failure and incomplete history remain UNKNOWN/retry. Closed or expired history authorises only mechanical generation replacement, never semantic resubmission or automatic re-drive.

Public callback delivery and new-generation start cannot participate in a Dynamo transaction. Recheck generation authority before dispatch, verify generation/occurrence on receipt, and prove the race with replacement cannot admit a stale semantic consequence. Describe this limitation honestly rather than claim an exactly-once provider call.

Existing `PipelineControlPlane.getAwaitSemanticCheckpoint(tenant, interaction)` and bounded execution Await reads are valid neutral observations: native recovery, operators and any host can inspect semantic identity, status and release without callback/ARN/history fields. Do not add an AWS-shaped read. If reconstruction needs a paginated execution recovery checkpoint, extend the owning control plane with semantic execution/continuation facts and a neutral cursor; prove both native and AWS use it. The current arbitrary 100-record read plus `findFirst` cannot imply completeness.

The runtime-owned Dynamo stream adapter should extract only a stable semantic lookup identity using the semantic store's codec. The AWS host then obtains current authoritative status through the existing control-plane read. Old/new Stream images are wake-up hints, never semantic authority. Provider-table events may be decoded by the AWS repository itself.

## 4 Handler generation

Keep `AwsDurableInputDecoderRenderer` and `PipelineGenerationPhase.generateAwsDurableInputDecoder` in compiler. They already generate the application decoder against the canonical REST DTO and reject streaming input. Generic host handlers stay in runtime artifacts. No reflection-based discovery or hard-coded application descriptors in production.

Preserve the present non-streaming support restriction explicitly. Decouple coordinator FUNCTION packaging validation from remote worker placement: FUNCTION coordinator/action entry points can dispatch signed SQS envelopes to COMPUTE or FUNCTION workers. Verify renderer role selection, remote-worker descriptors and actual packaged dependency closure. Do not redefine FUNCTION or force compute workers to acquire Lambda entry points.

## 5 AWS deployment and configuration

Deploy ingress, numbered Durable coordinator, release-pinned action host, queue adapters, semantic tables, mechanical table/index/Streams, targeted repair and bounded due-work timing together. Ingress authenticates through the application facade; invoke-only IAM is not application/tenant authorisation. Fix the same release's immutable coordinator, action and worker identities in a deployment manifest; a live alias selects new admissions only. Recovery selects code compatible with the retained TPF release and must not blindly use the latest alias.

Retain numbered versions and relevant function resources on replacement/deletion. Garbage collection requires no live execution and expiry of the longest recovery horizon, including TPF-retained executions after provider-history expiry. Retention of a version is insufficient if its action target or function is deleted.

Configuration contract includes the existing `TPF_AWS_DURABLE_ACTION_FUNCTION`, function/qualifier, binding table, action timeout and callback retention; process-loop disabling; queue URLs; semantic store providers/tables; signed worker and resume-token secrets. Validate required configuration before admission. The supplied SAM currently does not define all semantic provider/table settings: accept an explicit deployment-owned configuration mapping and validate it against the application's provider startup checks. Secret references belong in Secrets Manager/SSM with scoped reads; remove plaintext secret values from shared Lambda environment maps. Do not provision infrastructure in ordinary builds.

Use exact resource IAM for each role and each selected runtime provider. Allow Stream failure destination sends, transition response consumption, and semantic transactions required by real action paths. The AWS wake-up role should invoke semantic reads rather than directly read semantic tables. Separate coordination, action, worker and repair permissions; prove provider callback/history resource scoping on deployed AWS.

Explicitly encrypt SQS, mechanical/semantic tables, provider history, artifacts and logs. Customer-managed KMS configuration must include service/key policies and exact encrypt/decrypt grants; validate a CMK deployment in conformance. Keep tenant data, secrets, callback IDs and full provider history out of logs. PITR and deletion protection cover durable facts; callback TTL cleanup is independent of the semantic recovery horizon.

Bound worker concurrency, repair concurrency, polling/history pages and retry/backoff. For Lambda SQS mappings, visibility is at least six times function timeout plus batching window ([AWS configuration guidance](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-configure.html)). Attach DLQs/failure destinations and owned alarms to ingress async failure, Durable failure, all worker boundaries, Streams and EventBridge delivery/repair failure. Scheduled provider sweeping is not normal liveness: host due-work timing through bounded, idempotent semantic actions and event-driven wake-ups, with explicit timeout/continuation evidence.

Preflight checks region/runtime/SDK capability, account start/checkpoint/history rates and concurrency, execution lifetime, operation count, payload/state budgets and retention. Current AWS documentation specifies 3,000 operations and 100 MiB cumulative written state ([quotas](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html)); execution timeout 1–31,622,400 seconds and history retention 1–90 days ([configuration](https://docs.aws.amazon.com/lambda/latest/dg/durable-configuration.html)). Keep quota validation wired into deployment rather than leave `AwsDurableHostLimits` as an unused record. Include long-running operation budget exhaustion and supported successor-generation rollover behavior in tests.

## 6 Observability and operations

Correlate tenant-safe execution identity, pipeline/contract/release, mechanical generation/occurrence, provider execution ARN, action, attempt and outcome. Avoid execution IDs as metric dimensions. Emit bounded structured events for callback registration/binding, stale fencing, delivery classification, history unavailability, replacement and repair result. Correlation fields are not authority or credentials.

Metrics and alarms: callback-to-resume latency, outstanding admitted completions, binding conflicts/stale rejections, UNKNOWN classifications, replacements, repair age/failure, Stream iterator age/dropped batches, queue age/DLQs, Lambda errors/throttles, Durable operation/state budgets and retained-version inventory. Require an alarm destination and finite log retention. TPF retry/DLQ/effect evidence remains separate from provider mechanical failure telemetry.

Runbooks: retry uncertain inspection with bounded backoff; targeted reconstruction from TPF state; recover failed Stream/EventBridge delivery; drain and investigate worker DLQ before authorised semantic re-drive; restore semantic storage; retain/recover pinned code; rotate signing keys with old-execution compatibility. Never replay business effects because a provider callback is uncertain. Repair work retires only when durable semantic inspection shows consequences converged.

## 7 COMPUTE and FUNCTION compatibility

FUNCTION workers use SQS event sources and production batch adapters. COMPUTE workers consume the same signed request/response protocols with the same tenant, release, idempotency, retry/DLQ and uncertain-outcome rules. Supply a deployment choice that disables only transition-worker Lambda consumption when a compute consumer owns that queue; do not disable coordinator/action/Await admission hosts or create two competing consumers unintentionally.

Run equivalent conformance assertions for both placements, including duplicate delivery, failure, retry evidence, release mismatch, uncertain outcome and recovery. Current docs describe compute protocol reuse, but current SAM only instantiates Lambda workers and the compiler gate only proves FUNCTION coordinator packaging. Support claims must identify the proven mixed placement rather than claim all-COMPUTE generation or Spring parity. Direct Durable-to-worker invocation remains deferred.

## 8 Conformance migration

Keep ordinary PR builds deployment-free. Retain protected manual/nightly AWS execution and add an exact-candidate release gate. Preserve historical scenario IDs 1–22, seeded randomized races, provider-history-loss recovery, stale-generation tests and version-pinning assertions. The existing missing-history injection remains legitimate fault instrumentation; distinguish it from actual elapsed retention and keep the latter optional long-duration coverage.

Build a compiler-generated fixture application and deploy production host/deployment artifacts. Inject failures around calls or adapters, never replace registration, wake-up, repair or semantic decisions. Store artifact digests, runtime/compiler/BOM SHAs, region/capability checks, seed, scenario coverage, results and cleanup outcome in the evidence report. Add a deployment-free dependency/architecture check excluding proof packages from published artifacts and ensuring deployed handler symbols resolve to production code.

Migration gates: semantic actions first; production callback/replacement/repair next; generated release metadata and production SAM next; both worker placements and new uncovered scenarios last. Add second/sequential and concurrent Await, missing registration, GSI lag, >100 Await records, history pagination, pre-registration provider loss, suspended timeout/continuation, retained-version cleanup, CMK/IAM failures and operation-budget exhaustion. Historical scenario success alone does not cover these paths.

## 9 Small issue and PR sequence

| Slice | Scope and evidence | Acceptance |
| --- | --- | --- |
| A Targeted repair lookup | Existing host repository/reconciler; bounded GSI pagination and retry on lag; deterministic unit tests | Binding-first index page and temporarily missing registration cannot retire repair |
| B Deployment retention and delivery | Existing SAM and deployment contract tests; retained function/version, visibility headroom, encryption defaults and Stream destination IAM | Static contract validation and SAM lint; no AWS deployment in PR build |
| C Production semantic action conformance | Replace proof semantic action algorithms with a fault-decorated production delegate; preserve fixture release preparation and fault points | Existing proof adapter tests and production action tests pass against the same implementation |
| D Binding lifecycle completion | Generation/occurrence transactional contract, runtime-owned semantic stream adapter, complete targeted recovery locators and suspended due-work hosting | Deterministic crash/race tests including lost history and sequential/concurrent Await; no host semantic schema parsing |
| E Release and deployment completion | Release-pinned coordinator/action/worker recovery, COMPUTE deployment choice, validated provider configuration, CMK/secrets, alarms/logs/preflight; compiler only where role generation needs change | Compatibility set plus template/lint/security checks and mixed-placement smoke coverage |
| F Permanent production conformance | Generated fixture + production SAM/handlers; remove legacy substitutes from gated path; exact-candidate promotion evidence | Protected AWS lanes prove all retained and new scenarios on both placements |

Do not open issues, commit, push, deploy or publish merely to represent this sequence. Prepare local reviewable changes first. D is the owning-abstraction change, not an invitation to invent a sibling SPI. Split D only if its generation storage migration and neutral stream adapter can each pass independently without a temporary competing semantic path.

Owner-local validation: `./mvnw -pl :pipelineframework-aws-durable-coordination-host -am test -Dmaven.repo.local="$PWD/.m2/repository"`; add proof-action host with `-DskipITs verify` for C and deployment contract verification for B. Full canonical runtime verification and cross-repository compatibility sets gate integration; AWS evidence gates support/release separately.

Slices A, B and C are implemented in [runtime PR #38](https://github.com/The-Pipeline-Framework/pipelineframework-runtime/pull/38) on `codex/aws-durable-production-audit`, based on the pinned #37 head. The three commits separate targeted recovery, deployment safeguards and production action conformance. A retries index lag, bounded-page exhaustion, absent Await checkpoint and unresolved replacement admission. B retains coordinator code and mechanical recovery evidence, enables explicit encryption defaults, adds SQS retry headroom and grants the Stream failure destination send permission, with deployment contract tests. C removes duplicate proof semantic actions in favour of the production action implementation, preserving fixture release setup and fault boundaries.

Validation passed: targeted model/host/deployment/proof-action `verify -DskipITs` (75 Java tests, no failures or errors), SAM lint using the deployment directory's existing configuration, 36 documentation tests and the documentation build. No cloud resources were deployed. CodeRabbit CLI 0.7.6 was authenticated but rejected the review with HTTP 403, “You are not a member of the requested organization”; no CodeRabbit review result is available. Full runtime, cross-repository and real-AWS gates remain required for promotion. Slices D–F and the support checklist remain outstanding.

Graph impact for the initial host edits: repository MEDIUM, six direct dependents (actions, stream processor, wake-up, terminal reconciler, services, proof repository); stream processor LOW and wake-up LOW, normal Stream handler path affected. Recheck impact before later symbol changes. Do not treat this graph result as proof of AWS behavior.

## 10 Support acceptance criteria

- [ ] Prerequisite #37 merged and its exact runtime candidate included; bounded semantic operations retain restart/convergence coverage.
- [ ] All four production coordinates published and BOM/candidate closure verified without proof runtime substitutes.
- [ ] Conditional, occurrence-aware generation protocol proves replay, conflict and stale fencing under randomized races.
- [ ] Host uses semantic control-plane observations; no internal semantic-table decoding or scanning.
- [ ] Public provider inspection distinguishes success, closed/missing history and unknown; bounded retries and repair never retire unresolved work.
- [ ] Missing history, lost mechanical rows, pre-registration loss and nonterminal execution recovery converge from TPF state without semantic resubmit/re-drive.
- [ ] Sequential/concurrent Await, item continuation and timeout progress while Durable is suspended.
- [ ] Coordinator, action and workers remain release compatible through alias movement, history loss and version cleanup.
- [ ] Production deployment meets configuration, encryption, secret, tenant/IAM, retention, DLQ/destination, alarm and quota gates.
- [ ] Equivalent FUNCTION and COMPUTE SQS worker lanes pass; documented limits match compiler/runtime capability.
- [ ] All 22 historical scenarios, seeded races, history loss, stale fencing, version pinning and newly identified gaps pass against exact production artifacts.
- [ ] Native conformance remains green; ordinary PR builds deploy nothing; protected promotion reports exact candidates and cleanup.
- [ ] Operator runbooks and published support documentation reflect verified limits and recovery procedures.

Until these gates are evidenced, the merged artifact packaging and architectural promotion should not be interpreted as complete production support. No established architectural alternative needs reopening.
