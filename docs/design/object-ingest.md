# Object Ingest And Publish

Object ingest lets TPF admit files or object-store keys into a queue-async pipeline without making a business step list folders, poll S3, dedupe keys, or construct async execution ids.

Use it when an object arrival or object listing is the pipeline input. Business steps should receive a domain input such as `CsvPaymentsInputFile` or `RawDocument`; the object shell owns listing, filtering, payload references, identity, and async admission.

Object publish is the output-side counterpart. It lets terminal pipeline values become durable objects without making the final business step group records, name files, write to S3, retry duplicate writes, or report object-write lifecycle state.

## Ingest DSL

Declare the source at top level, then bind it to the pipeline input.

```yaml
sources:
  csv-payment-files:
    kind: object
    provider: filesystem
    location:
      root: ../input-csv-file-processing-svc/csv
      prefix: ""
    filter:
      include: ["*.csv"]
    poll:
      enabled: true
      interval: PT10S
      batchSize: 50
    identity:
      fields: [provider, container, key, etag]
    payload:
      mode: reference

input:
  from: csv-payment-files
  emits:
    type: org.pipelineframework.csv.common.domain.CsvPaymentsInputFile
    typeName: CsvPaymentsInputFile
    mapper: org.pipelineframework.csv.common.mapper.CsvPaymentFileObjectMapper
```

The first pipeline step input must match `input.emits.type` or `input.emits.typeName`.

### Standard input

`stdio` is an endpoint variant of the object connector, not a separate console integration. It reads one
EOF-delimited object from standard input and invokes the configured object mapper once. The mapper continues
to own deserialization and the typed input contract.

```yaml
sources:
  stdin-input:
    kind: object
    provider: stdio
    location: { endpoint: stdin }
    poll: { enabled: true, batchSize: 1 }
    payload: { mode: text }
```

The stream is non-seekable and is consumed once. A JSON object is a typical input; a JSON collection is also
valid when the declared input contract and existing mapper represent that collection. NDJSON, CSV record framing,
and multiple admissions from one stream are not supported by this endpoint.

```yaml
steps:
  - name: Process Csv Payments Input
    service: org.pipelineframework.csv.service.ProcessCsvPaymentsInputService
    cardinality: EXPANSION
    input: CsvPaymentsInputFile
```

## Projection Mapper

The mapper is explicit application code. TPF owns the object shell; the application owns how an object snapshot becomes a domain input.

```java
public final class CsvPaymentFileObjectMapper
    implements ObjectSnapshotMapper<CsvPaymentsInputFile> {

  @Override
  public CsvPaymentsInputFile map(ObjectSnapshot snapshot) {
    return new CsvPaymentsInputFile(new File(snapshot.localPath()));
  }
}
```

## Publish DSL

Declare the target at top level, then bind terminal pipeline output to it.

```yaml
publish:
  csv-payment-output-files:
    kind: object
    provider: filesystem
    location:
      root: ../input-csv-file-processing-svc/csv
    naming:
      keyTemplate: "{groupKey}.out"
    payload:
      contentType: text/csv
    grouping:
      maxOpenGroups: 1

output:
  to: csv-payment-output-files
  consumes:
    type: org.pipelineframework.csv.common.domain.PaymentOutput
    typeName: PaymentOutput
    mapper: org.pipelineframework.csv.common.mapper.CsvPaymentOutputPublishMapper
```

`output.to` attaches Object Publish to terminal pipeline output. It is not a user-authored step, so the pipeline still ends at the last business transition.

### Standard output

Use the same object publish mapping with a `stdio` target to write mapper-rendered payload bytes to standard output:

```yaml
publish:
  stdout-output:
    kind: object
    provider: stdio
    location: { endpoint: stdout }
```

TPF flushes the output session at completion and never closes stdout. Standard output is non-atomic: emitted bytes
cannot be rolled back after a failure. Keep diagnostics and framework logs on stderr so a command such as
`echo '{"name":"Mariano"}' | ./run-demo | jq .` remains machine-readable.

The `examples/stdio-object-demo` stdio object pipeline demo is the Unix endpoint reference. CSV
Payments remains the filesystem object ingest/publish reference, while Search includes the S3 object-ingest mapping.

```yaml
steps:
  - name: ProcessApprovedPaymentStatus
    service: org.pipelineframework.csv.service.ProcessApprovedPaymentStatusService
    cardinality: ONE_TO_ONE
    input: ApprovedPaymentStatus
    # accepts omitted — implicitly accepts ApprovedPaymentStatus
    output: ApprovedPaymentOutput

  - name: ProcessUnapprovedPaymentStatus
    service: org.pipelineframework.csv.service.ProcessUnapprovedPaymentStatusService
    cardinality: ONE_TO_ONE
    input: UnapprovedPaymentStatus
    # accepts omitted — implicitly accepts UnapprovedPaymentStatus
    output: UnapprovedPaymentOutput

  - name: Finalize Payment Output
    service: org.pipelineframework.csv.service.ProcessFinalizePaymentOutputService
    cardinality: ONE_TO_ONE
    input: PaymentOutputBranch
    accepts:
      - ApprovedPaymentOutput
      - UnapprovedPaymentOutput
    output: PaymentOutput
    # Required here: adapts the final local domain result to the object-output contract.
    # This is separate from output.consumes.mapper, which renders published object payloads.
    outboundMapper: org.pipelineframework.csv.common.mapper.PaymentOutputMapper
    terminal: true
```

The output contract must match the last step output type. For an object output, the terminal step also declares
`outboundMapper` when its local domain result must be adapted to that contract. The top-level
`output.consumes.mapper` has a different job: it renders the already-adapted terminal values into object payloads.
Neither mapper is a `java` binding. Local service and operator Java input/output types are inferred from their
signatures and mapper resolution; use `java` only for an explicit Java assertion or a framework-owned/remote binding.

## CSV Payments Shape

CSV Payments uses both sides of the object shell in the default path.

| Concern | Legacy file-step path | Connector-first path |
| --- | --- | --- |
| Source discovery | `ProcessFolderService` listed folders as a business step. | Object Ingest lists and admits source objects, then submits deterministic queue-async executions. |
| CSV parsing | `ProcessCsvPaymentsInputService` parsed the selected file. | `ProcessCsvPaymentsInputService` still parses the source object domain input. |
| Provider wait | `Await Payment Provider` dispatched one interaction per row. | Same authored await step; TPF coordinates itemized completion through durable await units and a live await session when the queue-async transition is active. |
| Output file | `ProcessCsvPaymentsOutputFileService` grouped and wrote final files. | Object Publish groups terminal `PaymentOutput` values and writes `{groupKey}.out`. |
| Reader pacing | `BlockingIteratorPacer` throttled the old path as a fallback. | The parser advances by reactive demand, the await in-flight window, and streaming publish backpressure. |

The business pipeline therefore ends at the last domain transition, not at a file-writing step:

```text
Object Ingest
  -> Process Csv Payments Input
  -> Await Payment Provider
  -> Process Approved Payment Status / Process Unapproved Payment Status
  -> Finalize Payment Output
  -> Object Publish
```

`Object Ingest` and `Object Publish` are framework-owned I/O shells around the pipeline. They are not replacement names for user-authored steps.

The parser pace in the connector-first path is reactive. `ProcessCsvPaymentsInputService` still owns CSV parsing, but it is requested by the pipeline as downstream capacity becomes available. It is not held back by the deprecated CSV demand pacer.

## Publish Mapper

Application code renders terminal values into object payload chunks. TPF owns grouping, key templating, provider selection, write idempotency, backpressure, telemetry, and lifecycle reporting.

```java
public final class CsvPaymentOutputPublishMapper
    implements StreamingObjectPublishMapper<PaymentOutput> {

  @Override
  public String groupKey(PaymentOutput item) {
    return item.getCsvPaymentsOutputFilename();
  }

  @Override
  public ObjectPublishGroupRenderer<PaymentOutput> openGroup(String groupKey, PaymentOutput firstItem) {
    return new ObjectPublishGroupRenderer<>() {
      private long count;

      @Override
      public String contentType() {
        return "text/csv";
      }

      @Override
      public ObjectPayloadChunk onItem(PaymentOutput item) {
        count++;
        return new ObjectPayloadChunk(renderCsvRow(item));
      }

      @Override
      public Map<String, String> finalMetadata() {
        return Map.of("recordCount", String.valueOf(count));
      }
    };
  }
}
```

## Connectors

Add the connector library where object ingest or publish runs:

```xml
<dependency>
    <groupId>org.pipelineframework</groupId>
    <artifactId>object-ingest-connector</artifactId>
    <version>${pipelineframework.version}</version>
</dependency>
```

V1 object source and target connectors:

| Connector | Purpose |
| --- | --- |
| `filesystem` | Local folders, tests, CSV-style batch inputs and output files. |
| `s3` | AWS S3-compatible object listing, text/reference payload admission, and object publication. |

The YAML field remains `provider` in v1 because it selects the Java `ObjectSourceProvider`
or `ObjectTargetProvider` implementation behind the connector. `ObjectTargetProvider` uses JDK
`CompletionStage`, not Mutiny or Quarkus types. The user-facing category is connectors because these
libraries own I/O boundary behavior, not pipeline side-effect semantics.

S3 text ingest example:

```yaml
sources:
  search-documents:
    kind: object
    provider: s3
    location:
      bucket: tpf-search-documents
      prefix: raw/
    filter:
      include: ["**/*.txt", "**/*.md", "**/*.html"]
    payload:
      mode: text
      maxBytes: 1048576
      charset: UTF-8
```

S3 publish example:

```yaml
publish:
  search-results:
    kind: object
    provider: s3
    location:
      bucket: tpf-search-results
      prefix: rendered/
      region: us-east-1
    naming:
      keyTemplate: "{groupKey}.json"
    payload:
      contentType: application/json
```

## Runtime Requirements

Object ingest v1 requires `pipeline.orchestrator.mode=QUEUE_ASYNC`. TPF submits each mapped input with a deterministic idempotency key derived from object identity, so duplicate listing results resolve to existing async executions.

Object Publish also targets queue-async terminal output. Streaming terminal output must use `StreamingObjectPublishMapper<T>`; the batch `ObjectPublishMapper<T>` remains for unary/small compatibility only. Publication happens before the queue-async execution is marked successful, so a successful execution does not silently miss its configured output object.

FUNCTION pipelines are rejected in v1. Quarkus currently hosts the bootstrap, but the ingest runner and provider SPI are plain Java so a Spring Boot host can wire the same semantics later.

## Observability Proof

Object I/O emits metrics for aggregate health and replay/span events for high-cardinality investigation.

Use metrics to answer SLO questions:

1. Are source objects being listed and admitted? Check `tpf.object_ingest.listed.objects.total`, `tpf.object_ingest.submitted.total`, `tpf.object_ingest.duplicate.total`, and `tpf.object_ingest.failed.total`.
2. Are terminal values being published? Check `tpf.object_publish.grouped.items.total`, `tpf.object_publish.published.total`, `tpf.object_publish.published.bytes.total`, `tpf.object_publish.failed.total`, and `tpf.object_publish.write.duration`.
3. Is the await boundary draining? Check `tpf.await.completion.admitted.total`, `tpf.await.completion.early_held.total`, `tpf.await.resume.released.total`, and `tpf.await.completion.dropped.total`.

Use replay to answer per-run questions:

1. Which source object was admitted?
2. Which await unit parked the execution?
3. Which completions were admitted, held, dropped, or released?
4. Which output object key was published?

The built-in CSV Payments replay is the reference connector-first proof. In the captured 1k run, Object Ingest admitted one source object, the approved and unapproved status branches started before all parser dispatch and await completions finished, `Finalize Payment Output` merged those paths, and Object Publish wrote the terminal output object before success. See [Replay And Live Topology](/operate/observability/replay#csv-payments-built-in-proof) for the measured timings.

See [Metrics](/operate/observability/metrics), [Await Boundary Operations](/operate/await-boundaries), and [Replay And Live Topology](/operate/observability/replay).

## Example Configs

- CSV Payments connector-owned input/output path: `examples/csv-payments/config/pipeline.yaml`
- Search S3 text ingest: `examples/search/config/pipeline.s3-object-ingest.yaml`

See [Field Materialization](/design/materialization) for related claim-check payload representation.
