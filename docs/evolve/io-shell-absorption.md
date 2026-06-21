# I/O Shell Absorption

::: tip Strategic Context
This page is the internal strategy companion to the public [Functional Core, Imperative Shell](/design/fcis) guide. Keep public docs focused on typed boundaries and connector usage; use this page for evolution decisions about which I/O shells TPF should absorb next.
:::

TPF should not replace Spring, Quarkus, or ordinary Java APIs. Its strongest adoption value is absorbing imperative I/O shells when framework semantics remove distributed-systems plumbing that application teams otherwise rebuild.

The benchmark is Await: a single await step absorbs pending interaction state, callback admission, correlation, continuation, timeout, duplicate completion handling, replay shape, and lifecycle observability.

## Decision Rule

| Concern | TPF posture |
| --- | --- |
| Calling a synchronous library/API | Pass through to Java, Spring, or Quarkus. |
| Hidden mutable inputs that affect decisions | Make explicit as guardrail or DSL boundary. |
| Replay, idempotency, correlation, continuation, or durable admission | Runtime primitive or framework-owned input boundary. |
| Payload representation without domain meaning change | Framework-owned representation shell. |

## Object Ingest V1

File/object/S3 ingest is the selected first follow-up because it removes common batch/background plumbing:

- folder or bucket listing
- include/exclude filtering
- ETag/version capture
- deterministic object identity
- duplicate admission handling
- payload references or text loading
- async execution submission

Filesystem and S3 support should ship as object source connectors. They are I/O boundary
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

## Guardrails

- V1 requires `QUEUE_ASYNC`.
- V1 rejects FUNCTION pipelines.
- The emitted input type must match the first step input.
- The mapper must implement `ObjectSnapshotMapper<T>`.
- The core runner and provider SPI are runtime-neutral; Quarkus only supplies the current lifecycle adapter.
