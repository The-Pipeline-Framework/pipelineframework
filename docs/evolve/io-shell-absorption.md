# I/O Shell Absorption Priorities

This note explores where TPF should absorb imperative I/O plumbing into framework semantics. It is an evolve document: it is useful for architecture and prioritisation, but it is not a public commitment that every capability below exists today.

The working value proposition is:

> Developers write business transitions. TPF absorbs the imperative shell when doing so removes real distributed-systems, replay, caching, persistence, correlation, or observability complexity.

This keeps TPF out of direct competition with Spring, Quarkus, and ordinary Java APIs. TPF should not wrap every external API. It should own boundaries where framework semantics materially improve correctness, replay, auditability, or operability.

## Posture Table

| I/O shell concern | TPF facility or capability | TPF posture | Adoption ROI | Why |
| --- | --- | --- | --- | --- |
| Deferred external completion: human approval, webhook callback, brokered provider result | Await step, await unit store, completion API, Kafka/webhook/interaction adapters | Runtime primitive and DSL step | Very high | Absorbs correlation, continuation, durable wait state, timeout, duplicate completion, replay shape, and observability. |
| DB/API reads that affect business decisions | Captured query step: `LoadX -> CapturedX`, then pure decision step | DSL step, first-party connector, and guardrails | Very high | Makes mutable external state explicit, replayable, auditable, and cacheable. |
| Long-running external jobs: submit job, poll/status, callback, final result | Job-await primitive or specialised await transport | Runtime primitive and DSL step | Very high | Removes polling loops, status stores, timeout handling, correlation, and resume plumbing. |
| Idempotent external commands: payments, emails, tickets, provisioning | Command/outbox step with command id, effect log, retry/DLQ, duplicate policy | Runtime primitive and DSL step | Very high | Targets one of the highest-cost reliability patterns in distributed applications. |
| File/object/S3 ingest | Object snapshot/list step with ETag/version, checkpointed listing, dedupe, quarantine | DSL step and runtime support | High | Common onboarding path for batch/background apps; removes listing/checkpoint/retry glue. |
| Large payload movement | Field materialization and repository providers | Framework-owned representation shell | High | Keeps the business type semantic while TPF handles claim-check storage, dereference, and checksums. |
| Persisting business outputs for UI/audit/replay | Persistence plugin, materialized/queryable records | Plugin and guardrails; possible DSL later | High | Strong existing value story; becomes stronger when query intent is explicit. |
| Expensive deterministic derived computation | Cache plugin, replay/cache policies, versioned keys | Guardrail and plugin | Medium-high | Valuable when paired with explicit captured query inputs; less distinctive as a standalone feature. |
| Cross-pipeline handoff | Checkpoint handoff | Runtime primitive or named boundary | Medium-high | Strong for larger systems, but usually later in adoption than Await or captured-query flows. |
| Auth/session/identity lookup | Explicit identity query step, claims mapper, policy guardrails | Guardrail first; DSL only when the read affects decisions | Medium | Important, but TPF should not replace Quarkus or Spring Security. |
| REST/gRPC/HTTP calls that return immediately | Operators, generated adapters, mappers | Pass-through with build-time guardrails | Medium | Useful for type/mapping/deployment portability, but not unique enough as an adoption wedge. |
| Message publish without waiting for result | Checkpoint publication, outbox-style command, or plain app code | Guardrail or command primitive depending on semantics | Medium | High ROI only when delivery, retry, idempotency, or ownership matters. |
| DB writes internal to a service transaction | Quarkus/Spring/JPA/reactive client | Pass-through | Low-medium | TPF should not replace normal transactional application code unless replay, idempotency, or audit is a pipeline concern. |
| Simple CRUD/admin APIs | Quarkus/Spring/Java | Pass-through | Low | Weak adoption lever and a distraction from TPF's semantic value. |
| Secrets/config/client setup | Quarkus/Spring/MicroProfile config, provider docs | Guardrail/docs only | Low | Necessary for production, but not a differentiating primitive. |

Use this decision rule:

| If the concern is mostly... | TPF should be... |
| --- | --- |
| Calling a library or API synchronously | Pass-through to Java, Quarkus, or Spring |
| Making hidden inputs explicit | Guardrail or DSL step |
| Preserving replay, audit, or cache semantics | DSL step with metadata |
| Owning correlation, continuation, timeout, duplicate admission, or recovery | Runtime primitive |
| Moving data representation without changing domain meaning | Framework-owned representation shell |
| Enforcing business correctness inside another system | Guardrail only; the target system must still participate |

## Object Ingest V1

File/object/S3 ingest is the first connector follow-up because it removes common batch/background plumbing:

- folder or bucket listing
- include/exclude filtering
- ETag/version capture
- deterministic object identity
- duplicate admission handling
- payload references or text loading
- async execution submission

Filesystem and S3 support ship as object source connectors. They are I/O boundary
capabilities, not pipeline side-effect plugins, even though the runtime-neutral Java SPI still
uses provider selection internally.

The v1 DSL uses top-level `sources` plus an input binding:

```yaml
sources:
  csv-payment-files:
    kind: object
    provider: filesystem
    location:
      root: ../input-csv-file-processing-svc/csv
    filter:
      include: ["*.csv"]
    poll:
      enabled: true
      interval: PT10S
      batchSize: 50
    payload:
      mode: reference

input:
  from: csv-payment-files
  emits:
    type: org.pipelineframework.csv.common.domain.CsvPaymentsInputFile
    typeName: CsvPaymentsInputFile
    mapper: org.pipelineframework.csv.common.mapper.CsvPaymentFileObjectMapper
```

This keeps object discovery out of business steps. In CSV Payments, the folder expansion step can be removed and the pipeline can start with `Process Csv Payments Input`. In Search, an S3 text source can emit `RawDocument` and start at `Parse Document`.

Guardrails:

- V1 requires `QUEUE_ASYNC`.
- V1 rejects FUNCTION pipelines.
- The emitted input type must match the first step input.
- The mapper must implement `ObjectSnapshotMapper<T>`.
- The core runner and provider SPI are runtime-neutral; Quarkus only supplies the current lifecycle adapter.

## Very High ROI Capabilities

### Deferred External Completion

Await is the benchmark capability. A `kind: await` step parks a `QUEUE_ASYNC` execution, creates durable interaction records, dispatches work through an adapter, admits a correlated completion, and resumes the owning execution from a typed payload.

Benefits to the user:

- Removes custom pending-interaction tables and callback controllers.
- Keeps the continuation pointer in TPF instead of application code.
- Gives a common model for human approval, webhook callback, and brokered request/reply.
- Makes duplicate completion and timeout behaviour visible as runtime semantics.
- Produces lifecycle events that operations and replay tooling can inspect.

Capability design:

- The authored await boundary remains a step in `pipeline.yaml`.
- The await unit is the durable completion contract.
- The await interaction is the transport-facing record.
- Transports handle dispatch/admission shape; they do not redefine resume semantics.
- Completion admission is typed and correlated by interaction id, correlation id, or signed resume token.
- External dispatch and provider-side effects remain at-least-once; business idempotency is still required.

DSL illustration:

```yaml
steps:
  - name: "Await Payment Provider"
    kind: "await"
    cardinality: "ONE_TO_ONE"
    input: "com.example.PaymentRequest"
    output: "com.example.PaymentDecision"
    timeout: "PT5M"
    idempotencyKeyFields: ["paymentId", "amount", "currency"]
    await:
      correlation:
        strategy: "signedResumeToken"
      transport:
        type: "kafka"
        request:
          topic: "payments.requests"
          key: "correlationId"
        response:
          topic: "payments.decisions"
```

Examples and inspiration:

- AWS Step Functions has callback tasks with `.waitForTaskToken` for workflows that pause until an external actor returns a token, and `.sync` integrations for job completion waiting: [service integration patterns](https://docs.aws.amazon.com/step-functions/latest/dg/connect-to-resource.html).
- Temporal positions durable workflow execution as the core primitive for long-running reliable processes: [Workflow Execution overview](https://docs.temporal.io/workflow-execution).
- Camunda job workers model external work that prevents process advancement until completed: [Camunda job workers](https://docs.camunda.io/docs/components/concepts/job-workers/).

Estimated effort:

- Current core: already implemented in meaningful form.
- Hardening: medium, mainly around additional transports, operational guides, and example coverage.
- New transport families: medium per transport when they preserve existing await semantics.

MCP-friendliness:

- Very high. The shape is schema-first and template-friendly.
- A generator can ask for external actor type, timeout, idempotency fields, transport, and completion payload, then produce a valid await step plus provider/client stubs.

Third-party integration priorities:

- Kafka remains important for brokered provider decisions.
- HTTP webhook remains the generic interoperability path.
- AWS SQS/SNS/EventBridge are high-value future adapters for cloud users.
- Slack, Teams, or email are useful only as human-approval surfaces; they should not change await semantics.

### Captured Query Steps For DB/API Reads

Decision-affecting reads should be explicit pipeline inputs. The pipeline should model:

```text
LoadCustomerRisk(customerId) -> CustomerRiskSnapshot
AssessCustomerRisk(CustomerRiskSnapshot) -> RiskDecision
```

instead of hiding mutable database state inside:

```text
AssessCustomerRisk(customerId) -> RiskDecision
```

Benefits to the user:

- Makes the real decision input visible in topology, replay, tests, and audit trails.
- Prevents retry/replay from silently using newer database state.
- Gives cache keys a real semantic object instead of a weak identifier.
- Separates I/O policy from business decision logic.
- Lets teams test decisions from captured query results without test databases.

Capability design:

- Use `kind: query` for framework-owned read boundaries that influence downstream business decisions.
- The first v1 connector is a first-party JPA connector, not a plugin and not application-supplied connector code.
- A query step owns an external read boundary and emits an immutable domain value. Naming that value `CustomerRiskSnapshot` is useful but not required.
- Query metadata includes query id, connector name, query version, input/output types, connector-specific spec, and capture key fields.
- Query steps are captured by default during managed TPF execution. On retry/replay of the same execution, TPF returns the captured result instead of calling the connector again.
- V1 intentionally does not expose `capture.mode`; `CAPTURED` is the only behavior. Refresh/live modes require a real future semantic choice.
- Build-time validation catches missing input/output types, unknown query ids, mapper/service/operator misuse, and non-`ONE_TO_ONE` cardinality.

DSL illustration:

```yaml
queries:
  customer-risk-by-id:
    connector: "jpa"
    input: "com.example.CustomerRiskLookup"
    output: "com.example.CustomerRiskFacts"
    version: "v1"
    jpa:
      entity: "com.example.CustomerRiskEntity"
      where:
        customerId: "input.customerId"
      projection:
        customerId: "customerId"
        riskBand: "riskBand"
        score: "score"
      result: "single"

steps:
  - name: "Load Customer Risk"
    kind: "query"
    cardinality: "ONE_TO_ONE"
    query: "customer-risk-by-id"
    input: "com.example.CustomerRiskLookup"
    output: "com.example.CustomerRiskFacts"
    capture:
      keyFields: ["customerId"]

  - name: "Assess Customer Risk"
    service: "com.example.risk.AssessCustomerRiskService"
    cardinality: "ONE_TO_ONE"
    input: "com.example.CustomerRiskFacts"
    output: "com.example.RiskDecision"
```

Examples and inspiration:

- Firestore uses `QuerySnapshot` for immutable query results; this is similar vocabulary, but TPF avoids naming the step itself `snapshot` because database/storage snapshots also mean backups or clones.
- Spanner read-only/snapshot-style reads and PostgreSQL MVCC snapshots show the broader industry use of point-in-time read views.
- Temporal workflows distinguish deterministic workflow logic from external activities; a TPF query step is not the same model, but the inspiration is the separation between durable decision logic and external effects.
- Event-sourcing and audit systems commonly persist the facts used for decisions; TPF offers a lighter pipeline-level captured read without requiring a full event-sourced application.

Estimated effort:

- Current first-class MVP: medium-large, covering DSL parsing, generated query client step, first-party JPA connector runtime, in-memory capture store, and captured replay semantics.
- Durable production stores beyond memory: medium per store.
- Provider-specific query catalogs/connectors: medium per provider family.
- Query observability and replay tooling polish: medium.

MCP-friendliness:

- Very high. Agents can inspect a pipeline, identify hidden decision reads, propose captured DTOs, and generate query-step YAML.
- A template generator can expose query reads as structured choices: connector, query id, version, input/output DTOs, connector-specific spec, and capture key fields.
- The schema deliberately avoids `capture.mode` in v1, reducing fake choices for agents and users.

Third-party integration priorities:

- PostgreSQL first, because it is a common system of record and has strong Java/Quarkus support.
- MySQL/MariaDB second for breadth.
- DynamoDB for AWS-heavy users and because TPF already uses DynamoDB in durable orchestration paths.
- OpenSearch/Elasticsearch for search/ranking snapshots where query results influence decisions.

### Long-Running External Jobs

Many external systems do not return the business result immediately. They return a job id, then require polling, event subscription, or callback admission. This is close to Await, but the provider lifecycle is job-shaped rather than arbitrary completion-shaped.

Benefits to the user:

- Removes custom polling loops and status tables.
- Centralises timeout, cancellation, retry, heartbeat, and final-result admission.
- Makes long-running provider work visible in topology and operations.
- Gives one model for cloud batch jobs, ML inference jobs, document processing, video/audio processing, and exports.

Capability design:

- Model as either `kind: job` or as a specialised await transport family.
- TPF should own submit result capture, job identity, status transitions, timeout, optional cancellation, final result loading, and resume payload.
- The provider adapter should own protocol specifics: submit call, status call/event, completion payload mapping, and cancellation call when supported.
- The execution should not busy-wait. Polling must be scheduled or event-driven through the orchestrator.

DSL illustration:

```yaml
steps:
  - name: "Run Statement Export"
    kind: "job"
    cardinality: "ONE_TO_ONE"
    input: "com.example.StatementExportRequest"
    output: "com.example.StatementExportResult"
    timeout: "PT2H"
    idempotencyKeyFields: ["customerId", "statementMonth"]
    job:
      provider: "aws-batch"
      submit:
        jobDefinition: "statement-export"
        queue: "exports"
      status:
        pollEvery: "PT30S"
      cancellation:
        onTimeout: true
```

Examples and inspiration:

- AWS Step Functions documents the `Run a Job (.sync)` pattern for integrations that wait for external work to complete: [integrating services](https://docs.aws.amazon.com/step-functions/latest/dg/integrate-services.html).
- Camunda job workers show the operational model of external workers completing process work.
- Temporal activities and child workflows show a code-first version of durable long-running work orchestration.

Estimated effort:

- Generic runtime model: large, unless implemented as a constrained await adapter first.
- One provider adapter: medium.
- Poll scheduling and cancellation semantics: medium-large.
- Operational docs and examples: medium.

MCP-friendliness:

- High. Agents can generate provider-specific job specs from intent, then bind request/result DTOs.
- The capability benefits from a provider catalog, because users should not hand-author every provider's status and terminal states.

Third-party integration priorities:

- AWS Batch and ECS tasks for cloud batch workloads.
- Kubernetes Jobs for portable infrastructure.
- AWS SageMaker and Glue where data/ML jobs dominate.
- Vendor-specific document/export APIs only after the generic job contract is stable.

### Idempotent External Commands And Outbox

Some I/O is not a read and not a wait. It is a command that changes another system: charge a card, send an email, create a ticket, provision a resource, post an event. These are high-risk because retries can duplicate real-world effects.

Benefits to the user:

- Gives every external command a stable business identity.
- Provides a durable effect record for retries, audit, and operator investigation.
- Reduces one-off outbox tables and duplicate-suppression code.
- Aligns command dispatch with TPF retry/DLQ semantics.
- Makes replay behaviour explicit: replay may observe the recorded command result instead of reissuing the effect.

Capability design:

- Introduce a command boundary with command id fields, dispatch policy, replay policy, and provider idempotency requirements.
- TPF owns command admission, effect record, retry scheduling, terminal failure routing, and observability.
- The target system must still provide or tolerate idempotency. TPF cannot guarantee exactly-once effects in a third-party system.
- A transactional outbox form should be available when the command is coupled to local durable state.
- Command result shape should distinguish accepted, completed, duplicate, rejected, and unknown outcomes.

DSL illustration:

```yaml
steps:
  - name: "Create Payment Charge"
    kind: "command"
    cardinality: "ONE_TO_ONE"
    input: "com.example.PaymentCommand"
    output: "com.example.PaymentCommandResult"
    command:
      provider: "stripe"
      idempotencyKeyFields: ["paymentId"]
      dispatch:
        retry:
          maxAttempts: 5
          backoff: "EXPONENTIAL_JITTER"
      replay:
        mode: "RECORDED_RESULT"
      failure:
        terminalSink: "execution-dlq"
```

Examples and inspiration:

- Stripe documents idempotent requests where clients send an idempotency key and repeated requests return the first result for that key: [Stripe idempotent requests](https://docs.stripe.com/api/idempotent_requests).
- Debezium's outbox event router documents the outbox pattern for avoiding inconsistencies between database state and events consumed by other services: [Debezium Outbox Event Router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html).
- Message brokers and workflow engines all expose variants of retry/DLQ handling, but TPF's adoption opportunity is typed command identity plus replay policy inside the pipeline.

Estimated effort:

- Conceptual and runtime model: large.
- Provider SPI and command store: large.
- First provider integration: medium.
- Transactional outbox integration: medium-large, especially if tied to local database transactions.

MCP-friendliness:

- Medium-high. Agents can generate command specs and idempotency keys, but the capability needs strong guardrails because unsafe generated commands could duplicate real effects.
- MCP tools should prefer dry-run, validation, provider capability discovery, and scaffold generation before live dispatch.

Third-party integration priorities:

- Stripe first as the clearest idempotency-key reference point.
- Kafka, SQS, and EventBridge for command/event publication.
- Debezium/PostgreSQL for transactional outbox.
- Email/SMS providers later, after the command result model and replay policy are stable.

### File/Object/S3 Ingest

File and object ingestion is a practical adoption path because many teams start with batch-like workloads even when the processing pipeline is reactive. The high-value shell is not `GetObject`; it is listing, dedupe, version capture, checkpointing, payload reference, and failure quarantine.

Benefits to the user:

- Turns object arrival or folder scanning into explicit pipeline input.
- Captures object version, ETag/checksum, size, timestamp, and source bucket/path as decision input.
- Avoids reprocessing the same object after retries or restarts.
- Provides a natural bridge to field materialization and payload repositories.
- Gives operations a clear quarantine/reject path for unreadable or invalid inputs.

Capability design:

- Provide an object-source step or source declaration that emits `ObjectSnapshot` records.
- Support event-driven ingestion and scheduled listing.
- Track processed object identity using bucket/key/version/ETag where available.
- Store large payloads by reference rather than pushing bytes through every step.
- Integrate with item reject sinks for malformed or unreadable objects.

DSL illustration:

```yaml
sources:
  csv-payment-files:
    kind: "object"
    provider: "s3"
    bucket: "incoming-payments"
    prefix: "csv/"
    identity:
      fields: ["bucket", "key", "versionId", "etag"]
    checkpoint:
      store: "orchestrator"
    payload:
      materialize: true
      field: "contentRef"

steps:
  - name: "Read Csv Payment File"
    source: "csv-payment-files"
    output: "com.example.CsvPaymentFileSnapshot"
```

Examples and inspiration:

- Amazon S3 event notifications can publish object-created events for operations such as `PUT`, `POST`, `COPY`, and multipart completion: [S3 event notification types](https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-how-to-event-types-and-destinations.html).
- S3 object metadata includes ETag, with caveats around multipart upload and encryption, which is useful but not a universal content checksum: [S3 Object API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html).
- Workflow and data orchestration tools commonly provide file sensors/operators, but TPF's opportunity is typed object snapshots plus replay and materialization.

Estimated effort:

- Source declaration and object snapshot DTO: medium.
- S3 event/listing provider: medium-large.
- Checkpoint/dedupe store: medium, likely reusable with orchestrator state.
- Cross-cloud providers: medium per provider after the core object identity model exists.

MCP-friendliness:

- High. Agents can generate object-source declarations from a bucket/prefix and produce DTOs, parsing steps, reject policies, and sample local/MinIO configs.
- It is especially friendly to template generation because file/object pipelines have predictable topology.

Third-party integration priorities:

- Amazon S3 first because it dominates object storage and has event notifications.
- MinIO for local and self-hosted compatibility.
- Google Cloud Storage and Azure Blob Storage after the identity/checkpoint contract is stable.
- Local filesystem provider for tests and small examples.

## Priority Recommendation

The adoption wedge should stay narrow:

1. Keep Await as the flagship proof that TPF can own a difficult I/O shell.
2. Add captured query steps as the next conceptual primitive because they clarify step design and the functional-core/imperative-shell boundary.
3. Treat long-running jobs as an Await-adjacent runtime primitive.
4. Treat idempotent commands/outbox as a separate reliability primitive with strong safety language.
5. Use object ingest as a practical example path that connects source handling, materialization, reject sinks, replay, and background execution.

Do not build a broad connector catalog before these semantics are clear. A connector without replay, identity, idempotency, or continuation semantics is usually just ordinary Java/Quarkus integration code.
