# AWS Durable production completion plan

## Current handoff — 15 October 2026

TPF Cloud owns deployment and will exercise AWS Durable with CSV Payments over
SQS HA as its first deployed application. Reuse that deployment for integration
evidence; this work does not create a separate ECS/Fargate fixture or Cloud producer.

| Owner | Next deliverable |
| --- | --- |
| Framework/proof work | Finish and review the local examples hardening; retain independent oracles and prepare the remaining provider-fault, retry/re-drive, release and migration checks |
| Release owners | Establish the compatible published contracts/compiler/runtime/BOM set; merged source and engineering caches do not establish released closure |
| TPF Cloud | Supply actual deployment/release identities, operation endpoints, scoped IAM/CMK/secret references, semantic-store readiness, and the real SQS consumer's identity and bounded pause/restore controls |
| Joint verification | Adapt ownership checks to Cloud's actual deployment mechanism, run selected faults with explicit authority, and retain exact revisions/digests, native/provider outcomes and restoration evidence |

The proof runner currently validates CloudFormation stack membership and disposable
owner tags. Cloud's deployment must provide authentic equivalent ownership evidence;
do not fabricate a stack ID or bypass the guard. Faults belong in an isolated test
deployment, with scope and restoration agreed by the Cloud owner. Real callback
expiry, throttling, history loss, generation races, retry exhaustion/re-drive,
two-release recovery, version retirement, CMK/IAM and quota evidence remain open.

Current state: coordinated repairs are merged. Local follow-up prepares five more
procedures (4, 5, 6, 10 and pre-registration loss); 88 Python and 28 Java tests,
fixture packaging/discovery and template checks pass. Examples follow-up source is
`3cfb221b303b21e716da8b0130f7a687f44b33a7`. There are 25 prepared FUNCTION catalogue IDs and 13 missing/partial;
all live production scenarios remain unexecuted. COMPUTE transition fault controls
and suspended continuation need the real consumer supplied by Cloud.

CSV's reported FUNCTION/REST union-DTO compilation failure is a separate compiler
gap; the merged finite-stream repair does not resolve it. The earlier six HTTP
404s also remain unexplained. A first successful Cloud deployment supplies
integration evidence; full support acceptance still follows the checklist below.

The remaining sections retain requirements, implementation decisions and evidence
history. Use this handoff for current coordination rather than reconstructing it
from the chronological audit.

The AWS Durable architecture is settled by ADR-0069. Complete the supported host already merged in runtime PR #34; do not extract another host or revisit semantic decomposition. The bounded, idempotent, recoverable action prerequisite is established. Durable facts must be replayed through inspection, reconciliation of missing consequences, and checkpoint convergence before work is retired.

## Baseline and audit result

Audit baseline, 6 October 2026:

- Documentation: `pipelineframework` main `b232cadb65fadae25a54f65ae17dea4eb039135c` (#998).
- Runtime: main `28dbeef9fed0f42b72384c27ad95f6ba2fc2b0c4` (#34), plus prerequisite PR #37 head `c18cd26805aeb0edbb9853c02c781891f9839720` in an isolated worktree. #37 remains open at audit time; its inclusion does not imply it is merged.
- Compiler: main `c5c29dab1b1c9238e56cfc2cc743765a4b8eb860`.
- GitNexus used the explicit remote-main indexes, then current source was verified. Runtime graph does not include #37; it is not evidence for that prerequisite. No index refresh or cloud deployment is required for this audit.

The production artifact shape and generic handlers exist. The table below records the audit-time gaps; the implementation status below records subsequent closures. Production support requires all remaining acceptance gates.

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

Current baseline, 12 October 2026: runtime prerequisite [#37](https://github.com/The-Pipeline-Framework/pipelineframework-runtime/pull/37) and productization [#38](https://github.com/The-Pipeline-Framework/pipelineframework-runtime/pull/38) are merged. Runtime [#45](https://github.com/The-Pipeline-Framework/pipelineframework-runtime/pull/45) replaces registration pagination with the dedicated `provider-registration-index`; [#46](https://github.com/The-Pipeline-Framework/pipelineframework-runtime/pull/46) keeps incomplete history inspection retryable. These are source changes, not new real-AWS conformance evidence.

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

TPF completion remains the sole semantic authority. AWS callback registration, binding and delivery receipts are disposable mechanical records in a separate table. Execution recovery locators and generation authority remain retained mechanical evidence for the recovery lifetime. No provider callback identifier enters `AwaitSemanticCheckpoint` or TPF Await records.

Retain immutable conditional writes and append-only records. Do not introduce `UpdateItem` or an upsert-based head. Extend the existing repository with transactional admission of immutable generation claims: generation one claims absent predecessor; replacement generation G+1 consumes exactly one immutable successor claim for G, and all registration/binding admissions condition on the claimed generation. Keep generation-fence evidence beyond callback TTL for the supported recovery lifetime; TTL deletion cannot re-authorise a stale generation.

Represent callback occurrence separately from provider generation. An execution can have multiple Await suspensions within one generation. Proposed registration key: execution scope + zero-padded generation + callback occurrence; binding key: interaction scope + generation + occurrence. Preserve existing generation-one/first-occurrence readers during a documented table migration, then remove transitional readers after retained executions drain. Include a schema version in new mechanical records and retain tenant-scoped, unambiguous identity encoding.

Repository contract:

- Claim successor generation conditionally and return explicit created/replayed/stale/conflicting outcomes.
- Register callback only for the admitted generation/occurrence. Equal semantic checkpoint and provider callback authority replay successfully regardless of housekeeping timestamps. Conflicting callback authority fails closed.
- Bind only against the matching registration and semantic identity observed through the control plane. Reject stale generation admissions atomically; latest-binding queries alone are not a fence.
- Record delivery evidence idempotently after provider acceptance or confirmed success in public history. A delivery receipt is mechanical evidence, never proof that TPF continuation work is complete.
- Query targeted registrations by provider ARN. The dedicated registration sort-key prefix removes the page-budget lookup failure. Index lag yields retry; it must not silently retire repair work.
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
| A Targeted repair lookup | Existing host repository/reconciler; dedicated registration GSI and retry on lag; deterministic unit tests | Binding-first index page and temporarily missing registration cannot retire repair |
| B Deployment retention and delivery | Existing SAM and deployment contract tests; retained function/version, visibility headroom, encryption defaults and Stream destination IAM | Static contract validation and SAM lint; no AWS deployment in PR build |
| C Production semantic action conformance | Replace proof semantic action algorithms with a fault-decorated production delegate; preserve fixture release preparation and fault points | Existing proof adapter tests and production action tests pass against the same implementation |
| D Binding lifecycle completion | Generation/occurrence transactional contract, runtime-owned semantic stream adapter, complete targeted recovery locators and suspended due-work hosting | Deterministic crash/race tests including lost history and sequential/concurrent Await; no host semantic schema parsing |
| E Release and deployment completion | Release-pinned coordinator/action/worker recovery, COMPUTE deployment choice, validated provider configuration, CMK/secrets, alarms/logs/preflight; compiler only where role generation needs change | Compatibility set plus template/lint/security checks and mixed-placement smoke coverage |
| F Permanent production conformance | Generated fixture + production SAM/handlers; remove legacy substitutes from gated path; exact-candidate promotion evidence | Protected AWS lanes prove all retained and new scenarios on both placements |

Do not open issues, commit, push, deploy or publish merely to represent this sequence. Prepare local reviewable changes first. D is the owning-abstraction change, not an invitation to invent a sibling SPI. Split D only if its generation storage migration and neutral stream adapter can each pass independently without a temporary competing semantic path.

Owner-local validation: `./mvnw -pl :pipelineframework-aws-durable-coordination-host -am test -Dmaven.repo.local="$PWD/.m2/repository"`; add proof-action host with `-DskipITs verify` for C and deployment contract verification for B. Full canonical runtime verification and cross-repository compatibility sets gate integration; AWS evidence gates support/release separately.

Slices A, B and C are implemented in [runtime PR #38](https://github.com/The-Pipeline-Framework/pipelineframework-runtime/pull/38) on `codex/aws-durable-production-audit`, based on the pinned #37 head. The three commits separate targeted recovery, deployment safeguards and production action conformance. A retries index lag, absent Await checkpoint and unresolved replacement admission. The later direct registration index removes the bounded-page lookup path. B retains coordinator code and mechanical recovery evidence, enables explicit encryption defaults, adds SQS retry headroom and grants the Stream failure destination send permission, with deployment contract tests. C removes duplicate proof semantic actions in favour of the production action implementation, preserving fixture release setup and fault boundaries.

Validation passed: targeted model/host/deployment/proof-action `verify -DskipITs` (75 Java tests, no failures or errors), SAM lint using the deployment directory's existing configuration, 36 documentation tests and the documentation build. No cloud resources were deployed. CodeRabbit CLI 0.7.6 was authenticated but rejected the review with HTTP 403, “You are not a member of the requested organization”; no CodeRabbit review result is available. Full runtime, cross-repository and real-AWS gates remain required for promotion. Slices D–F and the support checklist remain outstanding.

Graph impact for the initial host edits: repository MEDIUM, six direct dependents (actions, stream processor, wake-up, terminal reconciler, services, proof repository); stream processor LOW and wake-up LOW, normal Stream handler path affected. Recheck impact before later symbol changes. Do not treat this graph result as proof of AWS behavior.

### D1: immutable generation authority

The next runtime slice adds execution-scoped `CLAIM#G` and `SUCCESSOR#G` records to the existing mechanical table. A claim stores schema version 2 and pipeline/contract/release identity. Generation one conditionally creates its claim; G+1 atomically checks the matching predecessor claim, creates its own claim and writes the predecessor's immutable successor fence. These records have no callback TTL. Registration and binding transactions check that the claim exists and its successor fence is absent; binding also checks the registered callback authority. Strong reads classify replay, conflict and stale admission.

Replacement claims the next generation before starting the deterministic provider execution. A failed or uncertain start remains retryable: replay repeats the same start even though the old binding is fenced. Neither terminal reconciliation nor a completed-Await wake-up retires repair merely because the claim exists. A successor that is itself superseded does not start obsolete code; creating the following successor requires its predecessor claim and a callback registration/binding from that provider execution. Known-stale callbacks are not delivered. A callback call already in flight can race replacement because it cannot participate in the Dynamo transaction; semantic completion remains TPF authority.

The storage upgrade is intentionally fail-closed. Do not run unfenced retained action/wake-up/repair writers against this table alongside the new protocol. New installations create claims normally. Existing live executions need a quiesced, validated claim-chain migration, with semantic release identity checked through the control plane, before the new writers or event sources resume. Missing claims retry rather than silently bootstrap from callback TTL rows. Retained generations and fences cannot be removed until the execution's supported recovery lifetime has ended. Automated migration and coexistence with old retained host versions remain a rollout gate; this slice does not declare existing deployments safe to upgrade in place.

D1 retains the current registration and binding keys. Callback occurrence identity, complete reconstruction locators and suspended due-work timing remain D follow-ups. The independently reviewable semantic Stream boundary is described below. Local transaction tests cover concurrent old binding/new generation admission, replay/conflict and callback-row deletion; unit tests cover lost provider-start acknowledgement and repair retry. Real-AWS randomized conformance against these production artifacts remains required.

### D2: storage-owned semantic Stream observation

The semantic Stream path no longer decodes Await images in `AwsDurableAwaitStreamProcessor`. The runtime storage owner exports `DynamoAwaitInteractionKeyDecoder`, using the interaction store's own primary-key names, to produce only tenant and interaction locators. `AwsDurableHostServices` converts the Lambda key representation and reads the existing bounded `PipelineControlPlane.getAwaitSemanticCheckpoint` observation. The processor receives the current semantic checkpoint, not internal table fields. No new control-plane operation or multi-cloud SPI is introduced.

The read is provider neutral: it exposes TPF execution, interaction, correlation, status and pinned release identity without callback, provider-history or ARN fields. Native recovery and operators can use the same observation. Stream images are disposable notification evidence and may be stale; current semantic state controls whether binding/wake-up is appropriate. Missing checkpoints, malformed keys, read failures and unexpected source ARNs keep the record retryable. Known Stream removals and non-registration mechanical records do not create semantic completion.

Exact semantic and mechanical Stream ARNs are required host configuration and are wired to the matching SAM mappings. Tests cover stale/poisoned images, keys-only events, both arrival paths, retained-generation repair retry, missing checkpoints, semantic read failure, unknown source and removal handling. This closes production-host semantic image parsing; historical proof decoders still remain fixture code until conformance migrates to production handlers.

D2 left repeated-Await work open because an occurrence counter alone could not correct the first-checkpoint registration join. The following slice addresses sequential primary interactions; it does not close concurrent/itemised Await or the broader recovery gates.

### D3: sequential callback occurrence and semantic selection

The runtime adds `PipelineControlPlane.getCurrentAwaitSemanticCheckpoint(tenant, execution)`. This provider-neutral observation follows the execution's `WAITING_EXTERNAL` unit to its primary interaction using point reads. It preserves completed status while parent consequences remain outstanding. Native coordinator tests prove the observation without an AWS dependency or execution-history listing; admission runs before storage access. It exposes neither provider identities nor DynamoDB fields.

The driver checkpoints that interaction with the blocked status before creating a callback, then attaches its deterministic generation-local occurrence. Registration schema version 3 pins the interaction/correlation/occurrence; conditional binding checks those fields as well as provider authority and generation fences. First-occurrence storage keys remain readable; later keys include a padded occurrence suffix. Delivery evidence and callback receiver validation include occurrence. The registration index selects descending keys.

Registration Stream repair reads the pinned semantic interaction directly rather than the first of a bounded historical list. A completed interaction remains joinable when completion beats registration. A late semantic event cannot bind a newer callback. Terminal provider recovery locates the execution's current Await and, if it advanced before a later registration, uses the old provider record only to reconstruct a new generation, never to bind the old callback to the new interaction.

`CallbackOccurrenceMigrationValidation=validated` is required without a default. Operators must validate a fresh table or drain/isolate old callback executions and writers. New production registration requires a pinned interaction; legacy registration events and terminal repair fail closed. No automatic migration of rows, old serialized callback signals or Durable checkpoints is claimed. Retained host packages must remain protocol-compatible.

Local Durable driver tests, Stream tests, native semantic-read tests and LocalStack transaction tests cover sequential occurrences, immutable replay/conflict, cross-binding rejection, stale generations, late first completion, completion before registration and pre-registration provider loss for a later Await. Real-AWS conformance against these exact artifacts is still required. Concurrent/itemised Await, missing mechanical locators, suspended due-work timing, migration tooling and complete retained-version validation remain outstanding.

### D4: bounded due-work hosting during suspension (12 October candidate)

A production `AwsDurableDueWorkHandler` consumes scheduled SQS ticks and calls the existing `PipelineControlPlane.sweepOnce` with fresh host time. It does not inspect callback rows, provider history or semantic table schemas. The existing local control plane supplies the typed item-continuation handler, so timeout admission, due execution dispatch and durable continuation recovery retain their semantic owner. Tick payloads cannot select tenants, executions or deadlines.

The production SAM adds one-minute EventBridge delivery, a dedicated encrypted tick queue, Lambda with SQS maximum concurrency 2 and matching reserved capacity, bounded sweep limit, retry/DLQ delivery and queue-age/DLQ alarms. Its role grants semantic actions and SQS transition response consumption, with no callback or provider authority. Failed ticks are partial-batch retries; later scheduled ticks also re-inspect retained semantic work. This is normal liveness, not scheduled provider reconciliation.

Deployment-free handler and template tests prove the hosting contract; native sweep/timeout and item-continuation recovery tests remain the semantic reference. Protected real-AWS suspended timeout/continuation evidence is still required. Prompt provider callback settlement after TPF timeout/terminal failure, concurrent/itemised Await callback selection, missing recovery locators, complete release pinning and notification wiring remain open. This slice does not close the overall suspended-Await acceptance gate or claim new deployed evidence.

### D5: execution lifecycle, retained recovery and version pinning (12 October candidate)

This coherent runtime batch adds prompt terminal callback settlement through the
execution Stream and the late-registration Stream path. Both read the admitted
execution checkpoint, fence generation authority and classify uncertain delivery
from provider history. Failed callbacks carry only a mechanical terminal-status
error. They never complete a semantic Await, re-drive or replace a failed TPF
execution. Normal liveness uses Streams; exhausted records retain the existing
bounded retry/failure-destination contract.

`PipelineControlPlane.getExecutionSemanticCheckpoint` returns tenant, execution,
pipeline, contract, Release and status through the existing admitted point-read
owner. This provider-neutral observation supports native recovery and operator
inspection as well as AWS. Storage-owned key decoding keeps internal execution
DynamoDB schemas out of the host.

The driver registers an immutable `RECOVERY#<padded-generation>` locator before
polling or creating its first callback. It pins execution/Release and provider
execution identity, conditions on generation authority and has no callback TTL.
Targeted provider repair selects this prefix directly through the existing
provider-registration index. It reconstructs any supported nonterminal execution
without callback rows, and retries the same successor start after uncertain
acknowledgement. Terminal semantic executions do not start replacements.

Replacement starts use the original numbered coordinator version. The reference
SAM contract publishes and retains a numbered action version, captures it in the
coordinator environment and preserves invocation grants for retained versions.
`ExecutionRecoveryMigrationValidation=validated` requires fresh-host validation or
validated live locators/claims plus draining or isolation of old locator-free and
unqualified-action coordinators. It is an attestation, not automatic migration.

Deployment-free tests cover semantic read admission, late terminal registration,
SDK callback failure, history classification, non-Await recovery, callback-row
loss, start replay, stale generations and numeric target pinning. LocalStack
exercises the immutable recovery transaction and retained authority. No new
real-AWS evidence is claimed. The semantic-admission-to-locator window, loss of
retained authority, concurrent/itemised Await, complete retained-version upgrade,
COMPUTE deployment, operational wiring and protected AWS conformance remain open.

### Disposition of the 12 October PRs — 26 October 2026

Runtime #51/#52 were absorbed into merged runtime #58 on 14 October; the later
hardening work retained due-work hosting, terminal settlement and execution/version
locators, and added admission recovery. Their old branches were left open without
Git ancestry linking them to the replacement implementation. The descriptions
above preserve candidate-era evidence and limits; the completion audit below
records the subsequent implementation and remaining acceptance obligations.
Documentation #1019/#1020 recover deployment instructions omitted from merged
#1025 while preserving the later hardening and restricted-profile guidance.
This reconciliation adds no runtime behaviour or deployed conformance evidence.

## 10 Support acceptance criteria

### Completion audit: obligations frozen before new realization

The 12 October 2026 continuation audits runtime PRs #47/#48/#50 at merged main
`421ef428810917cf57eb97ab96a4cd5433bb5545` and open #51/#52 at candidate #52
`401990db65d30453742c74be9583aba431290ffa`. Requirements remain this plan,
the deployment contract and ADR-0069: original documentation pin
`a1f83ed1852f1ca0e587b87b7fb905540308afa3`, supplemented by documentation main
`37b2c48d87751710cd51326e86ffba44905948dd`. Foundry guidance is pinned to
`809b226ba691eacd4d7fedd65784e0dc0cae0b8b`.

Prior exposure: the implementation lead authored the earlier #45/#46 callback
pagination hardening and has read the supplied D1–D5 contribution summaries and
PR descriptions. This is retrospective, defect-informed verification, not a
claim of prospective discovery. New obligations below were recorded before
reading the new implementation and tests. Later amendments retain their timing.
The independent `adequacy` consultation assesses verification sufficiency;
the human requirements owner retains product acceptance. Implementation authors
cannot certify the support checklist themselves.

| Obligation | Original authority | Challenge and forbidden outcome | Independent oracle and cheapest evidence |
| --- | --- | --- | --- |
| D-GEN | §3 generation admission | Race successor claim with registration/binding and expire disposable rows; no stale authority or duplicate semantic consequence | Conditional transaction outcomes and final semantic facts, deterministic Dynamo component races |
| D-AWAIT | §3, §8 sequential/concurrent Await | More than 100 historical interactions, concurrent/item completions, stale Streams and completion before registration; no wrong interaction or premature parent release | Native semantic checkpoint and durable continuation facts through assembled runtime tests |
| D-RECOVERY | §3, §8 retained reconstruction | Lose history and mechanical registration before first/later wait, including nonterminal execution; no retired unresolved repair or semantic resubmit | Retained semantic state, stable generation/release and exactly one converged transition; component faults |
| D-TIMING | §5 suspended timing | Timeout, retry and item continuation while provider callback is suspended; no dependence on proof sweep or stranded terminal callback | Production due-work and execution Stream paths plus authoritative final semantic/provider state |
| E-RELEASE | §5, §7 retained code | Move aliases, delete/replace resources, recover after history loss; no unqualified incompatible action/worker or competing queue consumers | Immutable manifest/configuration and mixed-placement signed-envelope tests |
| E-DEPLOY | §5, §6 | Missing provider config, plaintext secrets, absent scoped CMK grants, notifications or quota gates; no admission to incomplete deployment | Template/lint/startup negative cases, real provider contract checks where static evidence cannot suffice |
| F-PRODUCTION | §8, §10 | Run historical IDs 1–22 and new gaps against generated fixture and production SAM/handlers; no proof semantic/recovery substitute in gated path | Production dependency/handler architecture checks plus exact-candidate protected AWS FUNCTION/COMPUTE conformance |

Ordinary builds remain deployment-free. Local passing evidence cannot close
AWS IAM/CMK/capability or exact-candidate promotion gates. The user clarified
that implementation uses merged main, with #51/#52 audited as candidates, and
set a zero cloud-spend limit. Locate existing E2E configuration read-only and
prepare the lane; do not launch cloud resources or paid runs. Real-AWS evidence
remains unsatisfied under this limit.
Raw logs and provider artifacts stay outside source control; report tested
revisions, commands, outcomes, limitations and available elapsed/cost accounting
here. LLM billing and human effort measurements are unavailable unless supplied.

### Executed retrospective evidence, 12 October 2026

The completion implementation is a dirty candidate over runtime `421ef428810917cf57eb97ab96a4cd5433bb5545`,
not a released compatibility set. PR #47 head `129e260c6eae295d527a17222592bcf0f1d67209`,
#48 `8039bb93815e2ae6faf96aa242310fedf9406683` and #50
`8647859539c95cf1626ccab09257e10d306b3676` are audited through merged main.
#51 `5a154f2cf832aab7c77ff7658a3bdf2cb1144c16` and #52
`401990db65d30453742c74be9583aba431290ffa` are candidate evidence; selected due-work,
terminal settlement and retained-recovery changes were reused without assuming acceptance.

| Obligation | Observed outcome and hardening | Evidence boundary |
| --- | --- | --- |
| D-AWAIT | Malformed mechanical registration was acknowledged. Three independently specified retry cases failed before the repair; all 15 Stream tests then passed. Unknown registration evidence now fails closed. | Production processor/batch result; substituted semantic reads, no AWS Stream proof |
| D-RECOVERY | Candidate #52 ordinary client/reconciler suite passed 23 tests, but the new crash-after-SUBMIT test failed while its registered-locator control passed (25 tests, one failure). Admission can commit before any locator is retained. | Actual driver/actions/repository/reconciler with external transports stubbed; the Dynamo fake did not enforce transactions |
| D-RECOVERY | Pre-admission immutable identity plus neutral client-key observation closes the lost-acknowledgement window without resubmitting semantics. The first repair incorrectly assumed generation-one authority existed; four component passes did not prove that assumption. An independent LocalStack probe failed with `ConditionalCheckFailed`. Establishing the validated original claim before its successor made the unchanged probe pass; adjacent read/recovery/transaction tests passed 46 tests, no failures/errors/skips. | Real repository and Dynamo transaction conditions in LocalStack; semantic observation and provider start remain substituted |
| D-AWAIT | Required shared Await-store paging, scoped opaque continuation and authoritative point reads replace a full unit traversal in one Durable action. More than 100 members, stale projection, filtered-empty continuation, foreign scope and prior-unit reset have focused coverage. Contracts verify passed 678 tests; focused runtime selection/store/read gate passed 131. A later independent probe exposed imported-identity collision and a prefix-order contradiction (two failures, baseline passed); typed tuple identity/opaque cursor made all 35 in-memory tests pass. A further actual Dynamo imported-GSI tie challenge passed with full last-evaluated-key continuation; the integrated paging gate passed 139 tests. | Contracts main `5c7a60c46b5f54c47e4846c194f9ab24390276a0` plus local changes; engineering artifacts only, no published provider compatibility claim |
| D-TIMING | The independent native due-work challenge exposed a committed `TIMED_OUT` interaction whose unit/parent/facts never recovered after a unit-store crash. The user approved the shared-store protocol repair. Timeout admission now atomically retains consequences, bounded discovery verifies native authority, required journal failures propagate, and acknowledgement follows convergence. The unchanged crash oracle, three post-commit cuts and completion-winning race pass. Provider discovery also exposed an unbounded active-deadline page walk (one failing adversarial test); one-page admission and retention-expiry cleanup made it pass. | Final targeted gate: 166 unit tests plus 19 actual Dynamo restart and six typed-import ITs, no failures/errors/skips. No provider/AWS timeout execution or historical backfill claim |
| E-DEPLOY | Explicit provider and secret-version configuration, encryption/grant attestations, bounded queues/concurrency, exclusive worker placement, retained numbered action, finite encrypted logs, notification destinations and actual mechanical outcome counters are implemented. Template/host/model gate passed 152 tests; telemetry passed nine. Subsequent required contract/artifact configuration checks passed six; repository/telemetry/real-generation/deployment gate passed 49. Final deployment gate passed 19, including retained numeric event-source versions, mandatory release isolation, publication identity and native provider operation/state alarms. | Template, startup mapping, LocalStack and lint; actual AWS IAM/CMK/quota/notification behavior remains unproved |
| F-PRODUCTION | Compiler-built two-Await fixture, production release bootstrap, external call fault proxy and exact-artifact gate replace historical proof authority in the new lane. Legacy promotion now rejects before cloud access. | Generated fixture architecture, proxy and negative gate tests; producer execution and protected AWS evidence remain required. Production beans must be discovered, not merely present in an archive. Scalar generated HTTP controls pass; the published compiler’s item streaming HTTP bridge stalls. The user approved a separate compiler repair; no production conformance claim follows from fixture packaging |

Reproduce selected gates from the runtime checkout with its own cache:

```bash
./mvnw -pl :pipelineframework-aws-durable-coordination-host -am verify \
  '-Dtest=AwsDurableAdmissionRecoveryTest,AwsDurableTerminalReconcilerTest,ExecutionReadModelTest' \
  -Dsurefire.failIfNoSpecifiedTests=false -Dit.test=AwsDurableGenerationFencingIT \
  -Dfailsafe.failIfNoSpecifiedTests=false -Dmaven.repo.local="$PWD/.m2/repository"
./mvnw -pl :pipelineframework -am test \
  '-Dtest=QueueAsyncCoordinatorTest,InMemoryAwaitInteractionStoreTest,DynamoAwaitInteractionStoreTest,ExecutionReadModelTest' \
  -Dsurefire.failIfNoSpecifiedTests=false -Dmaven.repo.local="$PWD/.m2/repository"
```

The timeout gate is reproducible after installing the coordinated local contracts
candidate into this worktree cache (engineering validation, not publication):

```bash
./mvnw -pl :pipelineframework -am verify \
  '-Dtest=AwaitItemContinuationRecoveryTest,AwaitTimeoutFlowTest,AwaitTimeoutPlanTest,InMemoryAwaitInteractionStoreTest,DynamoAwaitInteractionStoreTest,QueueAsyncCoordinatorTest,ExecutionReadModelTest' \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dit.test=AwaitRestartRecoveryIT,DynamoTypedAwaitImportIT \
  -Dfailsafe.failIfNoSpecifiedTests=false -Dmaven.repo.local="$PWD/.m2/repository"
python3 -m unittest discover -s aws-durable-proof/conformance -p 'test_production*.py'
```

The initial producer/gate boundary gate passed 18 local tests; a subsequent combined
Python gate passed 37 (including seven compiled-operation URL binding controls). These test fault procedures
and evidence rejection, and execute no AWS calls. Independent challenges found
an invented native `PENDING` precondition and a false-positive timeout oracle that
accepted unrelated parent failure. Corrections use native statuses, require the
exact `AWAIT_TIMEOUT` interaction error and validate pinned Stream mapping roles
before mutation. Scenario 19 currently proves explicit replay of an actual retained
registration record; it does not prove general scheduled discovery, and in-flight
consumers remain a causal limitation.

Subsequent independent resource-history tests passed 12 cases. Native history
reads now pass API `Marker`/`MaxItems` through CLI JSON input with automatic
pagination disabled, accept a callback on a later page, reject a later terminal
callback, and fail on invalid/repeated cursors or the finite page cap. CLI
`NextToken` was not itself a provider defect: automatic pagination has a different
CLI contract. The repair makes provider-page work and completeness explicit.
Logs: `/tmp/tpf-resource-history-boundary-green.log`.

The retained-version collector is advisory. It correlates the immutable manifest
with bounded native function-version/configuration reads, distinguishing missing,
mismatched, unmanifested and unknown versions without permitting cleanup or
claiming semantic execution inventory. Eight independent boundary tests pass.
Adversarial accepted-input probes found malformed falsy markers incorrectly
reported exhaustion and unknown signing fields copied into output; strict marker
validation and a verified-reference output whitelist close those local gaps.
These are response/report-boundary defects, not evidence that AWS supplied malformed
markers or that a generated manifest contained secret values. Logs:
`/tmp/tpf-retained-version-{falsy-marker,manifest-privacy}-probe.log`,
`/tmp/tpf-retained-version-advisory-green.log`.

The broader owner gate exposed the unchanged architecture guard: coordinator size
1093 exceeded 1050 lines. Moving read-only Await selection into the existing
`ExecutionReadModel` reduced the coordinator to 941 lines without changing the
limit or selection oracles. The affected read-model graph is HIGH risk; status,
registration and Durable polling share it. Coordinator 57, read-model 22 and
architecture eight focused tests pass. Full native verification remains unsatisfied;
`/tmp/tpf-await-read-model-fitness.log` is the focused evidence.

The approved FUNCTION HTTP repair is a separate dirty compiler candidate over
merged main `6f1b85582590e1cb1a03775f2add47b3b7411862`. The published compiler's
101-item Lambda bridge timed out while scalar controls passed. A finite JSON-list
response and paired generated client restore the existing typed stream; COMPUTE
retains SSE. Full compiler verify passed 925 tests, followed by 22 focused generation
checks. Actual regenerated FUNCTION HTTP tests passed six cases, including all 101
results through both the real generated client and actual CDI Step invocation; ordinary
COMPUTE HTTP/SSE controls passed five, and item contract/alignment tests passed two. Engineering artifacts are installed only in isolated caches. Logs:
`/tmp/aws-durable-generated-http-repaired-package4.log`,
`/tmp/aws-durable-generated-item-repaired-package.log`. Infinite streams and
responses exceeding host limits remain unsupported; these local bridge results
are not deployed AWS evidence. The final canonical `clean package` passed 2,194
native unit, 130 host and 19 deployment tests plus the generated fixture controls.
This package gate does not execute integration tests. A broader native `verify`
previously exposed fixture issues: inherited timeout tests lacked the deadline GSI,
global due queries shared unrelated scenario tables, and two test adapters lacked
new retained-work operations. Correcting fixtures without changing crash oracles
made the focused 87-unit/21-IT gate pass; full native verify remains pending.

The archived scalar and item candidates still compile operation URLs as
`UNCONFIGURED` and are intentionally rejected by the new pre-AWS binding check.
Preparation and promotion require the exact deployed operation host URL to match
all three literal generated client bindings in the actual archives. Seven new
URL boundary controls pass, including mismatches, placeholders and configuration
shadows. Evidence: `/tmp/aws-durable-operation-url-binding-evidence.json`,
`/tmp/aws-durable-operation-url-boundary-tests.log`. No endpoint was deployed.
Compiler source/JAR hashes and six-archive hashes are preserved in
`/tmp/aws-durable-f-engineering-evidence.json`; BOM source provenance is UNATTESTED.
A documentation SHA is not used as a BOM source revision.
Those archived runtime candidates predate the new E lifecycle/telemetry edits;
their hashes do not establish the current dirty candidate's package closure.

Before lifecycle implementation, E verification retains these original observability
obligations (section 6; adequacy owner is the independent reviewer, with human
acceptance retained):

- A winning semantic completion retains an immutable first-admission time. Replay,
  duplicate admission and later mutable status updates cannot replace it. Timeout
  and rejected completion cannot masquerade as admitted completion.
- Parent release durably acknowledges its unit/interaction scope at the release CAS.
  A crash after commit, lost acknowledgement and a later sequential Await must
  preserve that first release observation. Work is outstanding until authoritative
  release evidence exists; enqueue or a best-effort log is insufficient.
- An indexed bounded observation rechecks native authority and returns scoped opaque
  progress. Empty, filtered or stale pages cannot establish global zero outstanding
  work. No cross-page snapshot is promised. Foreign tenant/unit/release evidence
  must fail closed.
- Required timestamps/projections absent from older retained state remain an explicit
  migration/unknown condition. Tests must exercise crash cuts, deduplication,
  clock boundaries and fresh stores; they must not manufacture historical times.
- Semantic admission-to-parent-release latency and mechanical callback-to-driver-resume
  latency retain distinct start/end authorities. Missing provider history does not
  become a zero duration. Command outcomes require their own admitted/released
  semantics before entering an Await completion inventory.

Independent E consultation confirmed that exact outstanding admission inventory
and restart-safe admission/release latency cannot be obtained from counters,
mutable interaction timestamps or best-effort journal facts. A new semantic
lifecycle protocol requires atomic admission/release evidence across the owning
stores and a bounded observation contract. The user approved the lifecycle
protocol extension. Contracts over `5c7a60c46b5f54c47e4846c194f9ab24390276a0`
passed their full owner verification (688 tests). Native store admission and release
transactions, bounded strongly consistent primary-table OPEN queries, and neutral
read-owner observations are implemented as a provisional candidate. Ten memory,
four actual read-owner, eight Dynamo encoding/query and thirteen LocalStack lifecycle
cases passed; the two existing Dynamo store suites passed 24 and 50 tests.
The physical integration challenge initially failed nine cases because legacy
`ttl_epoch_s=0` was written and then treated as expired by CAS/index predicates.
Omitting that TTL attribute and accepting retained zero values made the unchanged
thirteen integration cases pass. This is confirmed compatibility repair, not an
oracle relaxation. Raw red evidence: `/tmp/tpf-completion-lifecycle-native-first.log`;
original/corrected reports: `/tmp/tpf-completion-lifecycle-{first,ttl-corrected}-results`.

The subsequent independent reissue probe is **confirmed failing**, not acceptance:
`AwaitCompletionReopenedAdmissionTest` executes native `REISSUE_COMMAND`, creates
and admits a distinct ordinary Await interaction in the previously completed unit,
and pauses before the replacement suspension CAS. The read owner erroneously
reports RELEASED and retires the new OPEN projection using the old unit ACK.
The probe failed both state and retained-work oracles in
`/tmp/tpf-completion-reopened-admission-red.log`. A nonnegative duration check cannot
establish causal coverage; primary membership alone changes during creation.
The shared Await occurrence/admission coverage repair was then escalated for
additional scope adjudication. At that stage, E lifecycle observations were
provisional and inadequate for authoritative inventory or cleanup decisions.
The approved causal continuation below records its local repair and remaining
production acceptance limits. The independent
adequacy owner remains `/root/adequacy`; human acceptance remains with the user.
Host-local counters are sampled query observations, never authoritative inventory.
The reviewable causal decision package is retained in
`/tmp/tpf-await-completion-lifecycle-test-consult.md`. It recommends a native Await
unit occurrence bound before external admission and preserved through worker
imports, item consequences and the exact parent CAS; it does not authorize
implementation or silently change ordinary Await replay behavior.

The affected native lifecycle/recovery gate then exposed a duplicate item-release
regression: an already-QUEUED parent constructed a new step-4 ACK and conflicted
with its retained step-2 ACK before reaching the normal CAS-loser behavior.
Requiring the strongly read parent to remain WAITING_EXTERNAL in the exact unit
before constructing the ACK restored that behavior; the transaction still checks
version, unit, status, TTL and release pins. The unchanged physical recovery
oracle passed, along with 96 unit tests and 46 integration tests covering lifecycle,
restart, typed imports and webhook/Kafka/SQS completion. Three unused mock stubs
were fixture adaptation errors. Red/corrected logs:
`/tmp/tpf-completion-lifecycle-affected-native-{verify,corrected-verify}.log`;
reports: `/tmp/tpf-completion-affected-native-{red,corrected}-results`.
Command: `./mvnw -pl runtime -am verify '-Dtest=AwaitCompletionLifecycleProtocolTest,AwaitCompletionObservationProtocolTest,DynamoAwaitCompletionLifecycleRepositoryTest,DynamoAwaitInteractionStoreTest,DynamoExecutionStateStoreTest' '-Dit.test=AwaitCompletionLifecycleProtocolIT,AwaitRestartRecoveryIT,DynamoTypedAwaitImportIT,WebhookCompletionDynamoIT,KafkaAwaitCompletionDynamoIT,SqsAwaitCompletionDynamoIT,AwsShapedCoordinatorActionsIT' -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false -Dmaven.repo.local="$PWD/.m2/repository"`.
This focused passing gate excludes the separately retained failing reissue probe;
it is not a passing full canonical runtime verification.

### Causal coverage continuation, 13 October 2026

The user approved explicit Await occurrence/admission coverage in shared contracts
and native stores. This is the next coordinated local repair, with no deployment,
publication or acceptance certificate. Prior knowledge includes the retained
reissue red and the independent decision package; later probes are defect-informed.
The original section 6 boundary remains authoritative. Independent adequacy stays
with `/root/adequacy`, while the implementation lead owns integration and the user
owns acceptance.

The chosen representation is one occurrence-qualified full unit identity carried
through existing exact unit-ID fields, rather than a second parallel lifecycle.
The execution owner persists the initial cause and creates a fresh cause at the
successful authorized reissue CAS. Retry/replay preserve it; page identity remains
part of the unit scope. Commands retain their own effect identities. Every parent
CAS and imported worker suspension must match the retained cause and page scope.
Qualified terminal units are sealed against fresh members, and their deduplication
replays the exact existing interaction. Item convergence retains exact interaction
completion keys rather than accepting only the same item index. Historical
observation reads its own retained unit/ACK independently of the current execution
cause. Legacy records without coverage remain UNKNOWN and keep unresolved OPEN
work; clocks and current membership cannot migrate them.

Required consequential checks: second admission before suspension remains OPEN;
second suspension/consequence/release and lost-response replay use their own ACK;
first-cycle observations remain immutable; equal/regressing clocks cannot prove
coverage; old completions, same-index children and worker imports cannot satisfy
another cycle; legacy and foreign-scope inputs fail closed. Both native providers,
signed wire round trips, context copies and Command retry/reissue controls are
compatibility obligations. Full native verification and published compatibility
sets remain separate gates. The local repair has bounded passing component evidence; the full canonical gate
remains separate. No red result is promoted to passing evidence before execution. The complete contracts reactor
passes 694 tests with the occurrence, retained-cause and wire contracts
(`/tmp/tpf-await-causal-contracts-verify.log`). Three runtime gates stopped before
causal test execution: initial-record constructor calls failed compilation, a new
context test named an absent enum constant, and two migrated item fixtures called
a missing helper overload. These mistakes were corrected; they are harness/integration
failures, not evidence for the causal oracle. Independent
source review then selected a non-atomic primary-attachment seal challenge. The
unchanged memory and physical LocalStack probes each fail once without harness
errors: open and dispatch-sealed primaries can change, and same-primary replay
mutates version/time. Dynamo also changes a terminal primary; memory already
preserves terminal state, correcting the broader source prediction. Logs:
`/tmp/tpf-await-primary-seal{,-physical}-red.log`; reports and frozen source hashes:
`/tmp/tpf-await-primary-seal-red-reports`. The affected gate below executes the
conditional repair. These store-level reds do not claim an executed end-to-end false-release race. Qualified
primary guards and coordinator winner checks repair the observed mutation in
source; exact replay is immutable and legacy configured-key deduplication remains
supported. Interaction creation and unit sealing remain separate writes, so this
work does not prove atomic item admission or orphan-cleanup liveness. A matching
terminal-failure ACK settles its failed occurrence without successful continuation
or resume latency. The
subsequent review found that the not-yet-reached unattached positive fixture used
a blank primary rather than the native absent state; setup now uses native creation
and sealing while preserving the consequence assertions. The docs gate also found
and corrected an ADR link typo; 36 tests and the full build with 64 public-content
checks pass (`/tmp/tpf-await-causal-docs-{tests,build-corrected}.log`).

The next canonical runtime gate reached 2,226 unit tests but stopped before ITs
on four fixture classes. Fresh causeful parents rejected old unqualified WAIT
identities; one helper embedded the wrong execution. Independent review confirmed
those setup failures, and also found a passing scalar fixture that ignored a
rejected WAIT and therefore did not establish suspended-release evidence. Fixtures
now bind the same actual retained cause across parent/unit/interaction and require
successful suspension; original ordering, crash and retry assertions remain.
The reissue (3), lifecycle (10), observation (5), memory unit (10), coordinator (35),
segment (8), context (12), signed gRPC (8) and worker (22) suites passed in that
incomplete run. These 113 passing tests do not make the full gate green. Log:
`/tmp/tpf-await-causal-runtime-canonical-corrected-verify.log`; fresh reports:
`/tmp/tpf-await-causal-canonical-fixture-results`. This is retained failed-gate
evidence; corrected physical execution follows below.

The affected native gate passed 353 unit tests and 53 of 56 physical IT cases,
including the corrected memory/physical seal oracle and recovery cuts. Three
physical reissue cases reached a real provider-validation failure: the native
REISSUE expression supplied unused `:dlq` and LocalStack rejected the request
before the reissue CAS. The unconditional value is also present on base main;
existing mock-based store tests did not validate expression binding usage. The
minimal repair includes that value only for the DLQ redrive branch. The unchanged
reissue gate then passed 51 native-store units, three reopened-admission units and
three physical reopened-admission cases, without failures, errors or skips. Log:
`/tmp/tpf-await-causal-reissue-physical-corrected-verify.log`; fresh reports:
`/tmp/tpf-await-causal-reissue-green-results`. Command:
`./mvnw -pl runtime -am verify '-Dtest=DynamoExecutionStateStoreTest,AwaitCompletionReopenedAdmissionTest' '-Dit.test=AwaitCompletionReopenedAdmissionIT' -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false -Dmaven.repo.local="$PWD/.m2/repository"`.
This is a confirmed native request defect,
not a fixture error or real-AWS result. Log: `/tmp/tpf-await-causal-native-affected-verify.log`;
fresh reports: `/tmp/tpf-await-causal-affected-red-results`. Source impact is UNKNOWN
in the stale index; native provider selection and actual test callers were checked.
The corrected source snapshot is `/tmp/tpf-await-causal-corrected-final-gate-source.json`: runtime
`05d0dc094708484af49b0e5baffa3dee77ff9143acd00bb5b5f4b910b402c2a3`, contracts
`32574e9b44aae058a65d2510237b65e72fd8bff734332664cc0b7c5855a66eb8`, compiler
`b23487d770493a6b2310014ef620fcbf0dbc258da8fafe6c991c9b4bbd89f3cd`.
These hash the sorted changed-file digest maps and exclude the pre-existing proof
README and initial-obligations publication exclusions. Contracts and compiler
artifacts are local unpublished engineering dependencies. Full canonical command:
`./mvnw verify -Dtpf.proof.stack= -Dmaven.repo.local="$PWD/.m2/repository"`; the blank
proof stack keeps the deployed-AWS lane inactive. Cloud resources created and
cloud spend remain zero; LLM, human and CI billing are unavailable. Independent
source and copied-report review supports the bounded local causal repair, including
old-ACK exclusion, own-occurrence release, historical observations, clock controls
and native seals. The full canonical result, publication/migration and live AWS
gates remain separate; see `/tmp/tpf-await-completion-lifecycle-test-consult.md`.

A full canonical run subsequently passed 2,251 unit cases and reached 134 physical
cases, with three failures confined to the five-case AWS-shaped fixture. Its shared
seed helper still imported an unqualified unit for a newly admitted causeful parent;
the actual worker's rejected suspension explains FAILED and the two absent callback
bridges. Independent source/XML review confirmed fixture classification and retained
the two passing retry/DLQ controls. The helper now derives the same qualified unit
from the actual retained execution cause/page across imports and the suspension;
restart, callback, pinning and no-business-replay assertions are unchanged.
Red log: `/tmp/tpf-await-causal-runtime-final-canonical-verify.log`; fresh reports:
`/tmp/tpf-await-causal-canonical-physical-red-results`. The corrected source snapshot
is `/tmp/tpf-await-causal-canonical-fixture-corrected-source.json`, runtime digest
`ed26c0ccebf9db7e84d8abaeada86488753ee72480809a5da9ae9f56c3e35c26`.
The corrected full canonical run passed all 2,251 initial unit cases and all 134
native IT cases, including the five AWS-shaped controls. It then reached 2,817
reported cases across the reactor and failed at six generated FUNCTION HTTP tests:
four failures and two errors, all actual 404 responses. Generated resources exist
with the expected routes and finite-list method. This failed run is not classified
as a fixture error or waived by prior focused HTTP success. Two existing deployment annotation-processor TODO tests were disabled.
Log: `/tmp/tpf-await-causal-runtime-final-canonical-corrected-verify.log`; fresh reports:
`/tmp/tpf-await-causal-canonical-generated-http-red-results`. A reached test count
does not establish complete verification. Final documentation checks passed 36 tests and the full build
with 64 public-content checks (`/tmp/tpf-await-causal-docs-final-{tests,build}.log`)
before this result-only update.

The same six generated HTTP tests then passed unchanged in a focused reactor run
(`/tmp/tpf-generated-http-discovery-focused.log`). One bounded full canonical rerun
completed with exit zero: **2,856 reported cases, zero failures/errors and two
pre-existing disabled annotation-processor TODO tests**. Fresh reports (439 XML):
`/tmp/tpf-await-causal-canonical-bounded-results/summary.json`; log:
`/tmp/tpf-await-causal-runtime-canonical-bounded-rerun.log`. The same full command
above left the deployed AWS lane inactive. Source snapshot:
`/tmp/tpf-await-causal-canonical-rerun-source.json`, runtime digest
`0b6abd9653a5ed94ecb932b23ec81be72d9bbc42fea4ef0714a8cda5c6744be9`;
contracts and compiler digests remain as recorded above. Only the two F Python
files changed between the prior failed Java gate and this rerun; Java, POM and
configuration bytes were unchanged. Passive listener PID/working-directory
observations at `/tmp/tpf-generated-http-port-owner.log` show the generated HTTP
module owning its listener during this passing run. They do not establish the
cause of the earlier 404s. That failure remains inconclusive context evidence,
retained separately from the bounded canonical pass. No further blind reruns
were selected. Local runtime/native integration is now green; protected exact
candidates, published dependency compatibility, retained-record migration and
production conformance remain independent acceptance gates.

Independent review subsequently corrected an overconstraint in its E gap register.
The original pinned section 6 requires useful metrics/alarms, bounded tenant-safe
correlation and durable convergence before repair retirement. It does not require
a globally exact snapshot inventory, samples for every lost acknowledgement or
provider-server timestamps. Scoped admission/ACK observations, verified host
API-ack-to-resume samples and retained first-observed repair age can meet their
stated bounded definitions; missing samples and repeated reads keep their limits.
This corrects an incorrect stronger hypothesis, without weakening the causal
release oracle or claiming live operational adequacy. The review found a real
source gap: OUTSTANDING completion observations lacked an alarm. The reference
SAM now routes that exact existing metric/dimension pair to the owned alarm topic,
with configurable five-minute observation-count threshold (default 100). Repeated
reads are not a population gauge; absent samples cannot establish global zero.
The existing 22 deployment tests and local SAM lint pass:
`/tmp/tpf-outstanding-completion-alarm-contracts.log`,
`/tmp/tpf-outstanding-completion-alarm-sam-lint.log`;
source digest/results: `/tmp/tpf-outstanding-completion-alarm-results.json`.
No global census or new clock protocol was selected. Live metric/alarm delivery,
released compatibility and retained migration remain original acceptance gates.

The subsequent host/model/deployment gate passed, including the actual local
Durable SDK runner, invalid-signal rejection and marker-RPC-failure convergence.
Its earlier reported 168-test XML inventory included unselected inspector reports;
that aggregate is superseded by the freshly filtered final gate below. Integration
tests were explicitly skipped. Immutable normal API-return
and first resumed-action observations now support a bounded callback latency
sample, with replay checkpoints and timing records isolated from Stream authority.
The callback client's public void API is preserved. Log:
`/tmp/tpf-callback-resume-observability-final-verify-success.log`.
This measures host API acknowledgement observation to action observation, not
exact provider acceptance-to-invocation latency; lost acknowledgements/history-only
delivery produce no sample, negative ordering is UNKNOWN, and claim-before-emission
can lose a metric on crash. Independent review found caller provider/release
attribution still unproved at that stage. The review corrected its own unsupported current-only
criterion: authentic superseded-generation historical observations are permitted,
but cannot be attributed to a successor or establish semantic authority.
Its subsequently frozen 11-case challenge confirmed seven false-attribution
failures: wrong/missing provider, pipeline, contract and release context all
created timing facts and emitted a sample. The authentic retained generation-one
control after a real generation-two claim passed. The driver now carries its
checkpoint and actual provider ARN using existing request fields; the action
compares those with the exact immutable registration before recording resume.
The paired regression is unchanged (SHA-256
`299710ff72e8627dc3c0f2cf250cc7ef4756e99b265ca091817c294fa7cc7c21`);
red log: `/tmp/tpf-callback-resume-attribution-canonical-red.log`.
Sampled completion UNKNOWN/native admission-to-release and callback sample/invalid
timestamp alarms require operator-supplied thresholds. The subsequent deployment
gate passed 21 tests in
`/tmp/tpf-callback-completion-observability-alarms-final.log`.
No sample or empty page establishes health or global zero outstanding work.

Repair-age realization is deliberately bounded to the first immutable host
observation of one provider ARN and age sampled on each targeted attempt.
Conditional first writes and strong duplicate reads preserve the original time
through retries/restarts; full ARN validation uses a non-index attribute. Negative,
missing, conflicting or unavailable observations produce UNKNOWN without delaying
repair. The new mechanical record is explicitly acknowledged by Streams. No
accepted replacement, provider-only outcome or sample retires semantic repair;
there is no durable open/close inventory or global oldest-age gauge. Five new
age cases and 19 Stream cases passed in `/tmp/tpf-repair-age-postcleanup-verify.log`.
Earlier attempts failed only test compilation/cache-topology adaptation, not
production oracles. The observation has no callback TTL and requires retained
mechanical cleanup authority; older attempts are not backfilled. Sampled age and
UNKNOWN alarms require `RepairAgeAlarmMilliseconds` and use the owned repair
notification destination. Retry exhaustion/DLQ alarms remain separate.

SAM lint exposed a real candidate deployment blocker that the template-reading
Java tests had not caught: a comma in the inline lifecycle-table Description was
parsed as another parameter property. Quoting the description made the same lint
command pass. Logs: `/tmp/tpf-durable-completion-final-sam-lint{,-corrected}.log`.
Command (deployment directory): `AWS_EC2_METADATA_DISABLED=true sam validate --lint --template-file src/main/resources/aws-durable-queue-async.yaml --region us-east-1`.
This is local template validation, not AWS IAM/encryption/alarm execution.

The final combined gate passed **208 selected Java tests**: 6 model, 153 host,
22 deployment and 27 legacy proof-action tests, followed by successful Quarkus
proof-action packaging. The paired attribution challenge remained byte-for-byte
unchanged and passed. Three existing host fixtures were adapted to the added
repair-age event while retaining exact exception, privacy and no-semantic-work
oracles. The first combined run failed those fixture expectations; after adaptation,
packaging exposed a missing application-owned input-decoder bean in the legacy
proof action host. Extracting its existing decoder into that bean restored packaging;
two decoder controls cover typed input and malformed/JSON-null rejection. This
legacy application adaptation does not put a proof decoder in production conformance.
Logs: `/tmp/tpf-durable-completion-final-host-proof-{verify,corrected-verify,packaged-verify}.log`.
Command (runtime checkout): `./mvnw -pl :pipelineframework-aws-durable-coordination-host,:pipelineframework-aws-durable-coordination-model,:pipelineframework-aws-durable-coordination-deployment,:pipelineframework-aws-durable-proof-action-host -am verify -DskipITs '-Dtest=AwsDurable*Test,Proof*Test' -Dsurefire.failIfNoSpecifiedTests=false -Dmaven.repo.local="$PWD/.m2/repository"`.
Selected XML reports and per-module totals are retained in
`/tmp/tpf-durable-completion-final-host-proof-results`; the class-prefix filter
excludes three stale, unselected `LambdaAwsDurableProviderExecutionInspectorTest`
reports. This gate skips integration tests. The earlier
`AwaitCompletionReopenedAdmissionTest` failure is historical and resolved: the
corrected reissue gate passed the reopened-admission unit and physical cases
recorded above. This host/proof gate is bounded passing evidence, not acceptance
certification.
Documentation tests (36), the full documentation build and its 64 public-content
checks also passed; logs: `/tmp/tpf-durable-completion-docs-final-{tests,build}.log`.

F's injected callback-failure diagnostic and procedure 18 now use actual production
callback/reconciliation and Stream failure boundaries. The diagnostic injects
callback failure, not elapsed expiry;
18 captures an actual decoder-rejected mechanical REGISTRATION and correlates
retained failure metadata to its Stream/shard/sequence/version. A fresh provider
contract probe found an empty Stream page incorrectly stopped retrieval despite
a live iterator. Its new controls failed before the bounded traversal correction;
closed shards and the global 100-read budget remain covered. This follows the
[GetRecords contract](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_GetRecords.html).
All 45 deployment-free Python boundary tests initially passed in
`/tmp/aws-durable-production-boundaries-stream-current.log`; red evidence is
`/tmp/aws-durable-stream-empty-page-red.log`. The subsequent independent acceptance
check traced original scenario 14 to a real 20-second callback timeout. Extraction
had removed that configuration while leaving its proof input flag/test. The new
injected-failure diagnostic therefore did not preserve its expiry variant, yet the
runner labelled it PASSED14 and the verifier accepted missing/injected/invalid
expiry variants. New runner and gate controls failed before the label correction.
Historical 14 now remains NOT_IMPLEMENTED with separate diagnostic status and
`elapsedExpiry:false`; promotion rejects PASSED14 unless its declared variant is
CALLBACK_TIMEOUT and `elapsedExpiry` is boolean true. The syntactically accepted
control is not executed timeout evidence. Actual CallbackTimedOut conformance
remains unimplemented; 18 is implemented but AWS-unexecuted. All 47 boundary tests
then passed in `/tmp/aws-durable-callback-expiry-label-green.log`;
red logs: `/tmp/aws-durable-callback-expiry-{runner,gate}-red.log`.
At that stage, twenty-two of the original 23 additional procedure gaps remained: partial 14 and
21 others. All live production scenarios remain unexecuted on this candidate.
One final disclosed two-shard probe found the empty first shard could consume
all 100 reads before the second shard's actual fault record was examined. The
unchanged global budget now rotates live iterators fairly. Both return-mode
controls failed before repair, then all 48 Python boundary tests passed in
`/tmp/aws-durable-stream-shard-fairness-green.log`; red evidence is
`/tmp/aws-durable-stream-shard-fairness-red.log`. Budget exhaustion remains
inconclusive and does not establish Stream completeness.
Two further source-backed production procedures are prepared after reconciliation
of the original 38 catalog IDs. Nonterminal provider loss pauses the exact owned
Work mapping before admission, observes a QUEUED native execution and retained
RECOVERY locator before its first Await, stops that real provider, invokes production
targeted reconciliation and verifies generation-two native identity, release pins
and final output after restoring delivery. Suspended continuation uses the generated
item deployment, verifies the real scheduled due-work wiring, blocks actual
transition delivery and Await Streams, observes three admitted completions with
pending retry work and no parent release/ACK while the provider is parked, then
requires exact child lineage, continuation facts, release ACK, typed order and
matching pinned provider output after delivery is restored. It invokes no caller
SWEEP. The terminal-item fixture has no downstream business suffix. COMPUTE
suspended continuation returns NOT_IMPLEMENTED because its exclusive consumer has
no owned pause/resume control; common Work mapping control supports nonterminal
loss for either placement.

All 55 actual-checkout Python test methods pass, with seven new boundary methods
and an additional permutation subtest. The permutation was accepted by the earlier
sorted-membership oracle; its new red confirms an ineffective ordering check, then
exact item sequence rejects it. Logs: `/tmp/tpf-f-two-procedures-order-red.log` and
`/tmp/tpf-f-two-procedures-ordered-boundaries.log`. Command:
`python3 -m unittest discover -s aws-durable-proof/conformance -p 'test_*.py' -v`.
Initial temporary preparation inserted tests into the wrong working directory and
ran them against the old helper, producing harness errors; the exact checkout
bytes were restored before integration. This is not behavioral defect evidence.
Independent review supports the prepared procedures' stated boundaries, not live
AWS success, nonempty-suffix execution or unique scheduled-invocation causality.
Source tests and no caller SWEEP cannot alone establish live scheduling causation.

Historical scenario 15 is also prepared using existing production entry points.
An authentic parked generation-one registration/Stream record is retained; the
pinned ingress resumes the same semantic execution at generation two. Completion
and replay of the stale registration must leave generation-two binding/pins intact,
produce no generation-one delivery, and converge one execution with exactly the
fixture's two Await interactions. Generation one must remain RUNNING with no
terminal callback history through generation-two success, before explicit cleanup.
No semantic re-drive/resubmit or proof recovery substitute is used. The actual
Python suite passes 56 methods, including ten scenario-15 boundary variants for
identity/pin changes, fabricated records, stale delivery and premature old-provider
termination (`/tmp/tpf-f-scenario15-boundaries.log`). This is offline procedure
preparation, not live fencing evidence or seeded randomized coverage. Java/POM
bytes remain unchanged from the passing canonical snapshot; this later Python
source is separately hashed in the current evidence manifest.

Historical scenario 7 FUNCTION preparation now uses a fixture-only SQS fault
handler around the actual production transition handler. It preserves the original
before-delegation and after-successful-action acknowledgement cuts. The producer
pauses the sole owned mapping before start, captures an authentic signed request,
enqueues unchanged duplicates and requires correlated native caller progress with
attempt still zero, plus exact lineage/pinned final result. An authentication-drop
ACK or later retry cannot substitute for that response. Seven Java tests exercise
the real production signed request/response boundary: send precedes lost ACK,
duplicates execute/publish once, and invalid authentication acknowledges without
publication. Legitimate nonce discard remains supported. All 60 Python methods
pass; scalar/item Quarkus packaging and archive inspection find both the fixture
wrapper and production delegate CDI beans. Derived fixture SAM lint passes with
only the inherited merge-anchor warning ignored. The first seven-test gate used
older cached binaries; it is superseded by the repeated current-artifact gate,
with exact old/new digests, commands and 12 source hashes in
`/tmp/tpf-f-scenario7-handoff.json`. Current runtime/host artifacts were warmed into
one ignored 839 MiB logical APFS cache clone; this is local engineering, not
publication. Production SAM is unchanged by the wrapper; conformance-only derived
wiring identifies its fault table/IAM/handler in the fixture manifest. Compiled
operation URLs remain UNCONFIGURED, so packaging/discovery is not a deployable
promotion candidate. These are local preparation tests, not AWS success, raw
response capture, unique scheduling or seeded randomized coverage.

The FUNCTION catalog now has 20 prepared procedure IDs, with 18 missing/partial IDs:
4, 5, 6, 10, 13, 14, 17, 20, 21, gsi-lag, history-pagination,
pre-registration-provider-loss, retained-version-cleanup, cmk-iam-failures,
operation-budget-exhaustion, seeded-generation-races, provider-history-loss and
release-version-pinning. COMPUTE additionally lacks scenario-7 delivery fault control
and suspended-continuation control.
All live production scenarios remain unexecuted. Most remaining cuts need actual
work/callback SDK fault boundaries or owned deployment controls; scenario 17's
alias movement conflicts with the current isolation attestation, retained cleanup
has no cleanup authority, and external CMKs are outside disposable stack mutation
authority. These require concrete scope/infrastructure adjudication before live
fault work; local substitute success cannot close them.

The apparent flat-HTTP versus nested-CLI callback-failure error mismatch was an
incorrect hypothesis: the official [CLI contract](https://docs.aws.amazon.com/cli/latest/reference/lambda/send-durable-execution-callback-failure.html)
and local `aws lambda send-durable-execution-callback-failure --generate-cli-skeleton input --no-sign-request --region us-east-1 --no-cli-pager`
both accept the nested `Error` structure. This offline check sends no API request.
The exact #51 candidate `5a154f2cf832aab7c77ff7658a3bdf2cb1144c16`
also passed its existing due-work handler (3), deployment contract (11) and
native sweep (8) tests in a clean checkout with its isolated dependency cache.
Command: `./mvnw -pl :pipelineframework-aws-durable-coordination-host,:pipelineframework-aws-durable-coordination-deployment -am test '-Dtest=AwsDurableDueWorkHandlerTest,AwsDurableDeploymentContractTest,QueueAsyncSweepFlowTest' -Dsurefire.failIfNoSpecifiedTests=false -Dmaven.repo.local="$PWD/.m2/repository"`.
This is bounded passing candidate evidence, not an executed timeout-crash or
provider-suspension challenge on #51. Log: `/tmp/tpf-candidate51-exact-focused.log`.
The current local source hashes and evidence references are captured in
`/tmp/tpf-durable-completion-current-evidence.json`; dirty snapshots are not
immutable published candidates. Cloud resources created and cloud spend remain
zero; LLM billing, CI billing and human effort are unavailable.

Raw timeout evidence: `/tmp/tpf-native-suspended-due-work-red.log`,
`/tmp/tpf-active-timeout-bounded-page-red.log`,
`/tmp/tpf-timeout-protocol-native-verify.log`. The original crash method SHA-256
remains `92fcad4ca965049151dc78f5b8cb4bca913032e30154cd8a36da7c9bbeefbec2`.
An initial race setup used completion at the deadline and correctly failed with
`AwaitInteractionTerminalException`; this was a fixture error, corrected to an
in-flight pre-deadline completion. Two older coordinator mocks required protocol
stubs; their null-return errors were harness adaptation failures. Neither is
reported as a production defect.

Raw evidence is local: `/tmp/tpf-durable-candidate52-{baseline,probe}.log`,
`/tmp/tpf-integrated-admission-generation-{red,green}.log`,
`/tmp/await-contracts-final-spi-verify.log`, `/tmp/await-runtime-final-focused.log`;
the independent consultation is `/tmp/tpf-aws-durable-completion-adequacy.md`.
Unchanged transaction-probe SHA-256 across red/green:
`25b3ddb96c286641608e2bf0f4c9524124c596c1e2c4ea783f8d06465e02ef04`.
Historical paging-only local SPI JAR SHA-256:
`2aac15cbe9c24fa5e129cd515ba4dea1480ce9287f90a74eee71239412a4e545`.
These hashes distinguish executed local artifacts from a Git revision or published release.

Timing is retrospective: frozen obligations preceded new realization; the independent
transaction challenge followed inspection of a passing repair and exposed an ineffective
storage fake. The ordinary candidate suite did not detect the admission gap. The prior
first-100 historical lookup hypothesis is not evidence against the scalar primary-interaction
path, which already uses a targeted semantic read. An unbounded full-unit lookup is an
ineffective alternative for a bounded action. Additional paging identity/order challenges
were investigated after implementation inspection, with raw red/green logs at
`/tmp/tpf-integrated-await-paging-{red,green}.log`.

The new paging SPI's initial global tuple-order promise was an implementation-era
overconstraint: legacy Dynamo physical index order does not satisfy it for prefix/delimiter
identities. Original D-AWAIT acceptance requires complete bounded traversal and correct
interaction selection, not the globally smallest pending item. Independent consumer review
confirmed native selection compares only one page, point-reads selected status and never
uses exhaustion as completion. The API now explicitly promises stable provider order,
complete unchanged-unit enumeration, lifecycle-stable positions and scoped continuation,
without a cross-page snapshot. Memory uses a typed tuple; Dynamo retains its existing index
and complete last-evaluated key. This deliberate unpublished contract correction does not
relax member completeness or require unrelated legacy index migration.

### Coordinated CI provisioning follow-up, 13 October 2026

Compatibility run [38328228077](https://github.com/The-Pipeline-Framework/pipelineframework/actions/runs/38328228077)
built the exact coordinated candidates successfully, but the examples and application
shards failed. The packaged OpenAPI callback returned 503 after restart, and CSV SQS
completion transactions raised `ResourceNotFoundException`. The lifecycle protocol
introduced a required table without updating those consumers' provisioning; native
owner tests already provisioned it, so their passing results missed this dependency.

The repaired example provisions `tpf_await_completion_lifecycle` with string `pk`/`sk`
keys plus the two recovery indexes used by current native stores. CSV's existing
bootstrap already has both recovery indexes; it now also creates the lifecycle table
and selects it explicitly in all three Dynamo-configured hosts. The historical AWS
proof template owns the same table and supplies its reference through the inherited
action environment. The production template continues to require an externally owned
table and its semantic IAM/KMS permissions; no table is created by runtime code.

The packaged callback was reproduced red with the current locally built runtime
(one test, 503), then passed unchanged completion/restart/deduplication oracles after
provisioning (one test, zero failures/errors/skips, 12.54 seconds). An earlier local
pass used the old cached runtime `ac426a70...` and is ineffective evidence for the
current protocol. Corrected dependency fingerprints are in
`/tmp/tpf-ci-lifecycle-repro-dependencies.json`; the runtime fingerprint is
`a3c19dc7a16087d847d77548b74c4706bfcdf5805265eb7df58de2151aacbde5`.
Red XML/process logs are retained in `/tmp/tpf-ci-packaged-callback-current-red-results`;
green output is `/tmp/tpf-ci-packaged-callback-repaired.log`.

Reproduce in the examples worktree after resolving the coordinated set:
`./mvnw -pl openapi-capability-proof/jobs -am verify -DskipUnitTests -Dit.test=PackagedJobCallbackIT -Dfailsafe.failIfNoSpecifiedTests=false -Dmaven.repo.local="$PWD/.m2/repository"`.
Actual LocalStack bootstrap and repeated bootstrap, conditional lifecycle primary-key
write, strongly consistent query and cleanup passed; all eight CSV interaction indexes
were ACTIVE. CSV launcher regressions passed eight tests. Runtime offline conformance
remained green at 60 tests; historical-template lint passed with only inherited YAML
merge warning `W1100` excluded. The plain SAM lint result including that warning is
retained separately. Complete CSV HA and the expanded exact compatibility set still
require CI evidence; these focused results do not establish production acceptance.
Local resources were disposable Docker/LocalStack only, with no real-AWS execution,
cloud resource creation, deployment or spend. README and initial-obligations changes
remain excluded from the coordinated PRs.

### Proof ownership and reviewable runtime stack, 13 October 2026

The user-authorized relocation moves `aws-durable-proof`, `spring-smoke-tests` and
`spring-blocking-smoke-tests` to examples #20. ADR 0072 refines the generated-application
proof boundary; runtime implementation tests remain with their semantic owner. This is
retrospective verification with prior implementation and CI-failure knowledge, reusing
the D/E/F obligations above rather than redefining acceptance. Independent adequacy owner
`/root/split_adequacy` found no confirmed relocation blocker; human acceptance remains
with the user. Its review is bounded to ownership, source coupling and evidence provenance.

Runtime review is a linear stack: [#56](https://github.com/The-Pipeline-Framework/pipelineframework-runtime/pull/56)
relocation (88 files, `d656de469d261f243d3e042b587a818383141247`),
[#57](https://github.com/The-Pipeline-Framework/pipelineframework-runtime/pull/57)
portable Await repairs (66 files, `b7754c9bfbd7da907eab7146716138fd7d552ed6`), and
[#58](https://github.com/The-Pipeline-Framework/pipelineframework-runtime/pull/58)
AWS host changes (49 files, `bba0bd15aacfad952e4ca4e83209b2a89cfa8137`). Each is below
CodeRabbit's observed 100-file cap. The top tree exactly equals the retained coordinated
snapshot (`99092cef0a4bfac8708253434fdc6f88bd0b4a30`); these layers require one top-of-stack
exact compatibility result and must not be merged separately. The original commits remain
recoverable in local `codex/aws-durable-pre-stack`.

The move exposed two invalid source-location assumptions: evidence inferred the runtime
SHA from the proof checkout, and an offline template check read sibling runtime source.
Production bundle preparation now requires the runtime build SHA, separately records
`examplesRevision`, and requires explicit runtime cleanliness attestation. Promotion checks
both producer and component identities. Offline tests consume the exact deployment JAR.
Two provenance regressions passed after their missing-capability red controls; the
original production scenario/support gates remain unchanged. The existing protected
workflow also moves to examples, but its environment protection, role/OIDC trust and
producer configuration still need owner configuration before any separately authorized
AWS run. No workflow or deployment was dispatched.

Deployment-free results: 10 Spring tests, 51 AWS proof Java tests and 62 Python conformance
tests passed; the deployed AWS suite executed zero tests without a requested stack.
Runtime publication checks passed seven regressions and verified 15 effective reactor
projects. Docs passed 36 tests and 64 public-content build checks. Spring's 28 non-POM files
and committed README bytes were preserved; local README content and initial-obligations
edits remain excluded. GitNexus did not cover the moved/deleted files completely; explicit
source manifests and tree equality supplement the graph result without treating its
unseen callers as unaffected.

Reproduce in examples after resolving the exact coordinated artifacts:
`./mvnw -f aws-durable-proof/pom.xml verify -Dtpf.proof.stack= -Dmaven.repo.local="$PWD/.m2/repository"`
and `./mvnw -pl spring-smoke-tests,spring-blocking-smoke-tests -am verify -Dmaven.repo.local="$PWD/.m2/repository"`.
The first attempt selecting only the proof aggregator executed no child tests and is
ineffective evidence. Stale locally cached host/deployment artifacts and a missing exec
plugin caused setup failures; actual dependency fingerprints were then recorded and the
real child reactor passed. Local logs/manifests remain under `/tmp/tpf-split-*` and
`/tmp/tpf-proof-split-*`, not in source control.

The provisioning repair's previous complete exact-set run
[38377513976](https://github.com/The-Pipeline-Framework/pipelineframework/actions/runs/38377513976)
passed all six product shards, resolving the earlier examples/CSV failures. It covers
runtime `23332dff2332b11d6d838ee4aa549277a0ccd4ef` and examples
`f75e545ddc03d542b3f3f86d07fd2b94a0ac554a`, not the relocated heads. Fresh exact-stack CI
is required. Automatic discovery selected runtime #58 and all three stack layers under
set `auto-620d58288badbe8d2c7a`, revision
`e12b04436b4b4c346c5eb1b95e5aaf3e643803e90f1e89169333e69c180f6ffa`,
with examples `59ff431ccdfed0547c9e9539bfa8958c2be1437d`. Local CodeRabbit review
of the host-only committed diff was blocked by organization membership (HTTP 403);
hosted review was rate limited, so neither supplies a completed new review. Protected real-AWS F conformance and the unchecked acceptance gates below
remain unproved. No merge, publication, deployment or real-AWS spend occurred; local
execution timing is in the logs, while CI/LLM billing and human effort are unavailable.

### PR 57 review follow-up, 14 October 2026

Review hints were verified against runtime layer `b7754c9bfbd7da907eab7146716138fd7d552ed6`.
New regressions reproduced terminal-unit store calls, timeout sweep abortion after one
interaction failure, and synchronous client-key checkpoint validation during assembly.
The repair skips terminal-unit writes and repeat telemetry while retaining admission
release, defers client-key validation/admission, and isolates each timeout attempt.
Failed consequences remain unacknowledged; later retained and newly due records continue
within the original cap. Discovery failures still fail the sweep. Crash-cut tests retain
their authoritative state and later-convergence oracles under the new per-record policy.
The focused gate passed 138 tests with no failures/errors/skips:
`./mvnw -o -pl runtime -Dtest=AwaitCoordinatorCompletionTest,AwaitTimeoutFlowTest,ExecutionReadModelTest,AwaitItemContinuationRecoveryTest,QueueAsyncCoordinatorTest -Dsurefire.failIfNoSpecifiedTests=false test -Dmaven.repo.local="$PWD/.m2/repository"`.
The actual Dynamo recovery suite also passed 15 tests with zero failures/errors/skips
(19.1 seconds), using `./mvnw -o -pl runtime verify -DskipUnitTests -Dit.test=AwaitItemContinuationRecoveryIT -Dfailsafe.failIfNoSpecifiedTests=false -Dmaven.repo.local="$PWD/.m2/repository"`.
The independent bounded adequacy owner `/root/lifecycle_upgrade_finding` found no
consequential blocker in these fixes; human acceptance remains unchanged. Runtime
review fixes are in `d4ac27de1d66ad953f6b8a404ce14a7960aa36e4` (#57), propagated to
`10f5e10693c39e1fb2d90aee44b5bbf8643f10e1` (#58), with the same 66/49-file layer diffs.
Logs and the initial red XML are retained under `/tmp/tpf-pr57-*`.

The infrastructure finding is already addressed by examples' proof table and CSV's
bootstrap; the runtime template deliberately takes externally provisioned semantic-table
ARNs. The Await setup page now explicitly orders table creation, ACTIVE verification,
transaction permissions and host rollout for an existing deployment. No duplicate semantic
table ownership or deployment was introduced. PR 1025 was rebased onto current main;
its unmerged Await occurrence ADR moved to 0073 because main already owns 0071 for diskless
payload delivery. Both decisions and their rationale remain intact.

Exact-stack run [38385366477](https://github.com/The-Pipeline-Framework/pipelineframework/actions/runs/38385366477)
passed the relocated `e12b04436b4b4c346c5eb1b95e5aaf3e643803e90f1e89169333e69c180f6ffa`
set. It precedes these review fixes and cannot certify their new heads. The three-layer
runtime stack and original D/E/F acceptance boundary remain unchanged; fresh exact-head
compatibility and protected real-AWS conformance remain separate gates.

### PR 58 logging and secret-gate follow-up, 14 October 2026

Verified review hints against runtime `10f5e10693c39e1fb2d90aee44b5bbf8643f10e1`.
The no-marker path admitted isolated secret-version/plaintext settings and an empty
compact deployment object; new parameterized tests reproduced six failures before the
repair. It now rejects deployment-owned secret references, versions, plaintext/ref signing
keys and compact configuration before any secret read, while a wholly unconfigured
ordinary build remains inert. This guard does not reinterpret generic runtime settings
as deployment ownership. Both existing pinned-secret success/failure tests remain active.

All ten host functions now explicitly route `LoggingConfig.LogGroup` to retained,
encrypted TPF log groups scoped by stack name and the stack ID instance suffix. The
logging regression covers destination linkage, unique names and stack-instance selection;
existing retention/encryption/alarm checks remain. The explicit refs establish creation
order, so redundant `DependsOn` entries were removed. The three focused classes passed
37 tests; offline CloudFormation lint passed with only the inherited YAML merge warning
`W1100` excluded. AWS's [logging configuration contract](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-properties-lambda-function-loggingconfig.html)
and [stack ID contract](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/pseudo-parameter-reference.html#cfn-pseudo-param-stackid)
were checked. Runtime fix commit is `479a0f503b7a9bb6ab51aa9d93f265006ae359f4`.
No live AWS logging or upgrade outcome is inferred from these deployment-free checks.

Compatibility run [38551365739](https://github.com/The-Pipeline-Framework/pipelineframework/actions/runs/38551365739)
failed the executed owned-payload HTTP fixture/source-equality check. Its deterministic
compiler merge `481dfe4f780b9249d73b6ffa5c49f0f04f08d9ea` combined PR #30 head
`24c3ee185c2b472da88fa575c598df9eb56fdf98` with base
`31cb08389ffbe47d7aac20203cb70e449e302831`, which emitted the newer raw multipart
upload. A minimal fixture refresh passed the unchanged equality oracle against that
exact renderer. During verification the compiler base advanced to
`6f1b85582590e1cb1a03775f2add47b3b7411862`; its renderer is byte-identical to the
original PR-head renderer and again matches the original fixture. The temporary refresh
was reverted instead of freezing historical output. Both source and HTTP fixture tests
passed against the restored shape; exact compatibility must be rerun on current bases.
This is a changed-baseline outcome, not permission to weaken the source-equality oracle.

Raw red/green reports and lint results remain under `/tmp/tpf-pr58-*`; the exact earlier
renderer oracle receipt is `/tmp/tpf-owned-payload-exact-merge-renderer/validation-receipt.json`.
Independent inspection confirmed the log linkage/encryption/retention and inert empty
configuration path. Broader direct-setting validation remains outside this secret-gate
repair. The original acceptance checklist and protected real-AWS gates remain open.

Cloud spend and resources created: zero. Local elapsed execution is available in logs;
CI billing, LLM billing and human effort are unavailable. Existing Search E2E applies and
destroys a different Lambda stack; it is configuration precedent, not evidence for this
Durable deployment or its cost. GitHub environment `aws-durable-proof` currently has no
protection rules. Protected exact-candidate AWS conformance, released cross-repository
compatibility, suspended concurrent-item consequences, retained version/key migration,
live scoped completion/repair observations, callback/resume metrics and alarm delivery, live operation/state/version measurements and released compiler HTTP streaming compatibility remain
unsatisfied. The checklist below deliberately remains independent of implementation progress.

### F continuation after coordinated merge, 14–15 October 2026

This is retrospective source preparation over examples `57505eb9fba2b0cb6345cdf174397ad670f5cb67`,
runtime `3414933c76f4fb3e46a66fe9c248ea684b069f61`, and documentation
`e77b2324e959c4810fb6b5d01711b7bfa6afd7e9`. The original pinned requirements,
historical scenario catalogue and unchecked acceptance below remain authoritative.
The lead disclosed prior repair/404 knowledge. Independent adequacy for these
changes belongs to `/root/f_compute_controls`; the user retains acceptance.

| Obligation from §§7–8 / historical catalogue | Preparation and forbidden false positive |
| --- | --- |
| 4/5: work failure before admission / commit before lost acknowledgement | Fixture delegates the real `AwsDurableWorkItemHandler`/`SqsWorkItemAction`; claims correlate authentic queue body, tenant and execution. For 5, production control-plane point reads must observe matching committed first-Await state **before** the fault claim. Eventual progress after a no-op ACK cannot prove this order |
| 6: transition response before lost acknowledgement | Isolate the existing AFTER_ACK cut, without injecting scenario-7 duplicates; require correlated native authenticated-response acceptance with attempt unchanged and pinned final output |
| 10: completion before binding | Pause both owned Stream mappings, observe real semantic COMPLETED while binding/delivery remain absent and provider callback remains unresolved, restore and require production binding, receipt and pinned convergence |
| Pre-registration provider loss | Immutable fixture HOLD blocks every registration retry before forwarding; retain authentic request/history/RECOVERY evidence, stop provider, release, run production repair and require successor authority. An explicit post-fence predecessor replay must return the exact typed stale-generation failure. Legitimate earlier predecessor rows may remain |
| Exact artifacts, semantic replay and item order | Match all ten manifested host roles and deployed code digests to inspected coordinator/action packages; bind scenario-12 replay to its checked manifest host, follow empty Stream pages with fair bounded traversal, and require numeric item order for both 3 and 101 results |

New probes confirmed ineffective oracles: reordered item output passed the old
sorted-membership check; valid empty Stream pages hid later records; foreign
tenant/correlation or replay targets could supply unrelated acknowledgements;
missing/incorrect host-package identities passed numeric-version-only checks.
Red/green logs are `/tmp/tpf-f-observation-{red,green}.log`,
`/tmp/tpf-f-replay-target-{red,green}.log` and
`/tmp/tpf-f-host-identity-{red,green}.log`. Independent reconstruction also showed
scenario 5 accepting an early no-op ACK followed by another delivery's eventual
commit. The repaired fixture reads authoritative committed state before claiming;
`/tmp/tpf-f-work-early-ack-{probe.py,red.log,green.log}` preserves that challenge.
The initial scenario-6 TypeError and intermediate tests run during worker edits
are harness/integration failures, not historical production defects.

Preparation now covers 25 FUNCTION catalogue IDs, leaving 13 missing/partial:
13, 14, 17, 20, 21, gsi-lag, history-pagination, retained-version-cleanup,
cmk-iam-failures, operation-budget-exhaustion, seeded-generation-races,
provider-history-loss and release-version-pinning. COMPUTE also lacks transition
6/7 and suspended-continuation controls. Its current generated fixture proves
HTTP/SSE, disables the SQS server and supplies no owned consumer/pause resource.
Repeating deterministic generation fencing does not prove the historical seeded
callback/throttling races. All live production scenarios remain unexecuted.

The work/transition templates have an explicit composite with both fixture
manifests retaining the original production SAM digest. Packaging/discovery is
still engineering evidence; operation URLs default to UNCONFIGURED. The standalone
action-proxy stack is rejected by the new HOLD membership check; an integrated
owned fixture or separately adjudicated auxiliary-stack authority is required.
The existing AWS workflow consumes promotion reports but has no production
execution producer. The relocated examples repository exposes no GitHub
environments, so protection/role configuration has not moved with the proof.

Release closure is blocked independently: runtime snapshot
[38635754617](https://github.com/The-Pipeline-Framework/pipelineframework-runtime/actions/runs/38635754617)
failed on Central connection reset; BOM snapshot
[38636067496](https://github.com/The-Pipeline-Framework/pipelineframework/actions/runs/38636067496)
failed because no GPG secret key was available. Compiler/contracts publishers
succeeded, which cannot substitute for the coordinated released set. No publisher
was retried. The earlier HTTP 404 remains unexplained; later passing controls
cannot establish its cause. Retained-record/key migration, two-release recovery,
version retirement authority, CMK faults, live quota measurements and cleanup
remain open. The user explicitly kept the proposed ECS/Fargate consumer and trusted
production producer infrastructure expansion out of scope. Those dependent gates
remain open; no deployment, publication, workflow dispatch or spend is authorized here.

The versioned [continuation evidence summary](/evidence/aws-durable/f-continuation-2026-10-15.json)
retains the non-sensitive commands, prerequisites, all 16 evaluated source hashes,
package hashes, outcomes, limitations and available timings. Its evaluated examples
revision is `3cfb221b303b21e716da8b0130f7a687f44b33a7`; it does not depend on the
temporary working copy. Reproduction requires Java 21 and the matching prewarmed
engineering cache; that cache was not archived, so the commands do not promise a
fresh offline build or byte-identical package reconstruction.

| Recorded gate | Outcome | Available local timing |
| --- | --- | --- |
| Python procedure/oracle tests | 88 passed | 0.348 s |
| Java fault-boundary / FUNCTION HTTP / COMPUTE HTTP controls | 17 / 6 / 5 passed; no failures, errors or skips | Unavailable |
| Scalar/item packaging, CDI discovery and derived-template lint | Passed; packaging skipped tests; lint ignored only W1100 | Unavailable |
| Documentation tests / build | 36 passed / 64 public-content checks passed; build preceded the final scope annotation | 446.8 ms / 83.14 s |

The raw files are preserved separately in an owner-local restricted archive
(directory mode 0700, bundle mode 0600), identified by
`aws-durable/f-continuation-2026-10-15/continuation-evidence.tar.gz`.
Its SHA-256 is `22a5e7363b00852a46f8aa34af7274641ac9ab2ce08c71d5965b129e0ca83ccd`;
the summary records the receipt and retention limits. The user selected local
archival on 15 October 2026; resolve this archive ID under `~/.codex/artifacts/`.
The committed summary is the handoff record, with raw logs retained as an optional
fallback for investigating historical results. No shared archive or remote URL is
required by this retention decision; live acceptance gaps remain unchanged.

Cloud resources and spend are zero; billed CI/LLM costs and human effort are
unavailable. README and initial-obligations edits are excluded. Source preparation
is not acceptance certification and does not change the checklist.

- [x] Prerequisite #37 merged; bounded semantic operations retain restart/convergence coverage. Exact candidate inclusion is required again for each production conformance run.
- [ ] All four production coordinates published and BOM/candidate closure verified without proof runtime substitutes.
- [ ] Conditional, occurrence-aware generation protocol proves replay, conflict and stale fencing under randomized races.
- [x] Production host uses semantic control-plane observations; storage-owned key decoding replaces internal semantic-image parsing, with no semantic-table scans. Production conformance migration remains a separate gate.
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
