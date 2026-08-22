# Object Ingest And Publish

Object ingest lets TPF admit files or object-store keys into a queue-async pipeline without making a business step list folders, poll S3, dedupe keys, or construct async execution ids.

Use it when an object arrival or object listing is the pipeline input. Business steps should receive a domain input such as `CsvPaymentsInputFile` or `RawDocument`; the object shell owns listing, filtering, payload references, identity, and async admission.

Object publish is the output-side counterpart. It lets terminal pipeline values become durable objects without making the final business step group records, name files, write to S3, retry duplicate writes, or report object-write lifecycle state.

## Ingest DSL

Declare the source at top level, then bind it to the pipeline input.

```yaml
connectors:
  local-files:
    provider: filesystem.objects
    version: 1

sources:
  csv-payment-files:
    kind: object
    provider: filesystem
    binding: local-files
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

### Grouped selection

Object Ingest normally admits each listed object separately. Add `selection.mode: together` when one
poll should admit the selected snapshots as one typed pipeline input. Explicit keys map canonical
record fields to source object keys:

```yaml
input:
  from: documents
  selection:
    mode: together
    keys:
      invoice: invoice.pdf
      attachment: attachment.pdf
  emits:
    type: com.example.documents.DocumentSet
    typeName: DocumentSet
```

For a homogeneous selection, `into` names the repeated `payload_ref` field that receives the ordered
references. Source filtering still determines the candidate set, and the existing poll/admission
lifecycle determines whether an empty, singleton, or larger set is available. A singleton is not a
separate error condition. Without `selection`, the existing one-object-per-admission behavior is
unchanged.

#### Example: admit a named document bundle

Use `keys` when the objects have different roles. Field names belong to the canonical contract; source
keys remain boundary configuration:

```yaml
types:
  DocumentSet:
    fields:
      - [invoice, payload_ref]
      - [attachment, payload_ref]
  DocumentManifest:
    fields:
      - [invoiceKey, string]
      - [attachmentType, string]

input:
  from: incoming-documents
  selection:
    mode: together
    keys:
      invoice: invoice.pdf
      attachment: terms.pdf
  emits:
    type: com.example.documents.DocumentSet
    typeName: DocumentSet

steps:
  - name: Describe document bundle
    service: com.example.documents.DescribeDocumentSetService
    cardinality: ONE_TO_ONE
    input: DocumentSet
    output: DocumentManifest
```

TPF generates the selection mapper. The service receives the typed bundle, not `ObjectSnapshot` or a
provider-specific filesystem/S3 location:

```java
@ApplicationScoped
@PipelineStep
public final class DescribeDocumentSetService
        implements ReactiveService<DocumentSet, DocumentManifest> {

    @Override
    public Uni<DocumentManifest> process(DocumentSet input) {
        return Uni.createFrom().item(new DocumentManifest(
            input.invoice().key(),
            input.attachment().contentType()));
    }
}
```

#### Example: admit a homogeneous batch

Use `into` when every selected object has the same role. In the repeated-field form, the value of
`repeated` is the element type:

```yaml
types:
  DocumentBatch:
    fields:
      - name: documents
        repeated: payload_ref
  BatchIndex:
    fields:
      - [count, int32]
      - name: keys
        repeated: string

input:
  from: daily-scans
  selection:
    mode: together
    into: documents
  emits:
    type: com.example.scans.DocumentBatch
    typeName: DocumentBatch

steps:
  - name: Index scan batch
    service: com.example.scans.IndexScanBatchService
    cardinality: ONE_TO_ONE
    input: DocumentBatch
    output: BatchIndex
```

```java
@ApplicationScoped
@PipelineStep
public final class IndexScanBatchService
        implements ReactiveService<DocumentBatch, BatchIndex> {

    @Override
    public Uni<BatchIndex> process(DocumentBatch input) {
        var keys = input.documents().stream().map(PayloadReference::key).toList();
        return Uni.createFrom().item(new BatchIndex(keys.size(), keys));
    }
}
```

Filtering and polling decide which snapshots are present. The service needs no special branch for a
one-element list.

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
    cardinality: ONE_TO_MANY
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

The `localPath` projection above is retained for existing filesystem applications, but it is not a
portable content contract. A neutral downstream connector consumes `snapshot.reference()` through
`PayloadMaterializer`; only the binding-owned object-source operation interprets `container`,
`key`, or other provider location metadata. The filesystem and S3 object sources support this bounded
materialization path. Standard input rejects reference materialization; an unsupported source fails
explicitly rather than leaking its location into application mapper code.

Filesystem materialization reopens the referenced file, enforces `maxBytes` before and during the
read, verifies its SHA-256 checksum, closes the input before completion, and returns immutable
bytes. It is repeatable while the referenced file and binding provenance remain unchanged. Standard
input remains single-consumption and cannot truthfully offer repeatable reference materialization
without first transferring ownership to durable storage.

S3 materialization uses the canonical bucket, key, optional version ID, size, and ETag captured in the
reference. It enforces `maxBytes` against the captured size, current object metadata, and returned bytes.
Ingested references use the S3 ETag as their immutable object fingerprint; Object Publish references use
TPF's SHA-256 content checksum. A mismatch fails instead of silently materializing a different object.
The bytes are staged into the same invocation-scoped local workspace as any other provider, so authored
file services remain storage neutral.

## Ordinary File Services

The `file` representation lets an ordinary business service work with `java.nio.file.Path` while the
canonical pipeline contract remains a `payload_ref`. Both boundary records opt into the same mapping;
no application `Mapper` or new step kind is involved:

```yaml
types:
  SourceDocument:
    fields: [[content, payload_ref]]
    mappings:
      file:
        type: java.nio.file.Path
        options: { maxBytes: 52428800 }
  RenderedDocument:
    fields: [[content, payload_ref]]
    mappings:
      file:
        type: java.nio.file.Path
        options:
          target: rendered-documents
          maxBytes: 52428800
```

The generated facade materializes bounded bytes into an invocation-scoped workspace, calls the
authored `Path -> Path` service, publishes its result through the named Object Publish target, and
returns the resulting `PayloadReference`. `ONE_TO_MANY` uses the same adapter with a `Path -> Multi<Path>`
service. Output paths must resolve to regular files inside that workspace; cleanup runs on completion,
failure, and cancellation.

The publish target must declare the Connector binding that owns the returned reference. The source
must likewise declare its binding when its reference will cross into another connector. The default
v1 publication key is the output filename; `options.key` overrides it when a stable application key
is required. Filename-derived keys are a default convention, not pipeline semantics.

### Materialize several named inputs for a typed service

A grouped record may map several `payload_ref` fields to one ordinary Java record of `Path` values.
This is an input-only representation: the authored service returns an ordinary typed pipeline value,
so no files are republished on its output boundary.

```yaml
types:
  InvoiceFiles:
    fields:
      - [documentId, uuid]
      - [originalFilename, string]
      - [invoice, payload_ref]
      - [catalogue, payload_ref]
    mappings:
      file:
        type: com.example.invoice.MaterializedInvoiceFiles
        options:
          fields: [documentId, originalFilename, invoice, catalogue]
          # Optional when the record also carries a reference unchanged for downstream use.
          # materializeFields: [invoice, catalogue]
          maxBytes: 52428800
  InvoiceAnalysisRequest:
    fields:
      - [documentId, uuid]
      - [invoiceText, string]

steps:
  - name: Prepare invoice analysis
    service: com.example.invoice.PrepareInvoiceAnalysisService
    cardinality: ONE_TO_ONE
    input: InvoiceFiles
    output: InvoiceAnalysisRequest
```

```java
public record MaterializedInvoiceFiles(
    UUID documentId,
    String originalFilename,
    Path invoice,
    Path catalogue
) {}

@ApplicationScoped
@PipelineStep
public final class PrepareInvoiceAnalysisService
        implements ReactiveService<MaterializedInvoiceFiles, InvoiceAnalysisRequest> {

    @Override
    public Uni<InvoiceAnalysisRequest> process(MaterializedInvoiceFiles files) {
        return Uni.createFrom().item(() -> prepare(files.invoice(), files.catalogue()));
    }
}
```

`options.fields` is ordered and must name every field in the canonical record in the representation
record's constructor order. By default every `payload_ref` field becomes a `Path`; `materializeFields`
may select a subset, leaving unselected `payload_ref` fields reference-typed in the representation
record. Ordinary scalar fields pass through unchanged. The generated facade materializes the selected
references into one invocation workspace, applies `maxBytes` to their combined materialized size, invokes the service, and removes the workspace
on completion, failure, or cancellation. The authored service receives no connector registry,
materializer, provider location, or storage-specific object.

### Example: normalize one file

The pipeline contract remains records containing `payload_ref`, while the authored step sees only
`Path`. The output target can be filesystem, S3, or another Object Publish provider:

```yaml
publish:
  normalized-documents:
    kind: object
    provider: filesystem
    binding: local-files
    location: { root: ./out/normalized }

types:
  SourceDocument:
    fields: [[content, payload_ref]]
    mappings:
      file:
        type: java.nio.file.Path
        options: { maxBytes: 10485760 }
  NormalizedDocument:
    fields: [[content, payload_ref]]
    mappings:
      file:
        type: java.nio.file.Path
        options:
          target: normalized-documents
          maxBytes: 10485760

steps:
  - name: Normalize document
    service: com.example.documents.NormalizeDocumentService
    cardinality: ONE_TO_ONE
    input: SourceDocument
    output: NormalizedDocument
```

```java
@ApplicationScoped
@PipelineStep
public final class NormalizeDocumentService implements ReactiveService<Path, Path> {

    @Override
    public Uni<Path> process(Path input) {
        return Uni.createFrom().item(() -> {
            Path output = input.resolveSibling("normalized-" + input.getFileName());
            Files.writeString(output, Files.readString(input).strip());
            return output;
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }
}
```

`normalized-...` becomes the default publication key because it is the returned filename. For a
singleton destination such as `latest.txt`, set `options.key: latest.txt` on the output mapping.

Structured records may also carry ordinary context across a file transformation. Declare ordered
`fields` on both mappings, select the input references to stage with `materializeFields`, and select
the returned `Path` fields to publish with `publishFields`. The authored input/output records replace
only those selected fields with `Path`; scalar fields and unselected `payload_ref` fields pass through
unchanged. `carryFields` may name unchanged canonical fields to copy directly from input to output,
so the authored result need only contain newly produced values. This form is ONE_TO_ONE and keeps correlation data beside a conditionally produced file
without exposing connector or materializer APIs to the service.

### Example: expand one archive into many files

`ONE_TO_MANY` lets one materialized file produce several published objects. Each returned path is
published independently, so meaningful output filenames make useful default keys:

```yaml
publish:
  extracted-documents:
    kind: object
    provider: filesystem
    binding: local-files
    location: { root: ./out/extracted }

types:
  UploadedArchive:
    fields: [[content, payload_ref]]
    mappings:
      file:
        type: java.nio.file.Path
        options: { maxBytes: 104857600 }
  ExtractedDocument:
    fields: [[content, payload_ref]]
    mappings:
      file:
        type: java.nio.file.Path
        options:
          target: extracted-documents
          maxBytes: 20971520

steps:
  - name: Extract archive
    service: com.example.archive.ExtractArchiveService
    cardinality: ONE_TO_MANY
    input: UploadedArchive
    output: ExtractedDocument
```

```java
@ApplicationScoped
@PipelineStep
public final class ExtractArchiveService implements ReactiveStreamingService<Path, Path> {

    @Override
    public Multi<Path> process(Path archive) {
        return Multi.createFrom().deferred(() ->
            Multi.createFrom().iterable(extractIntoSiblingFiles(archive)))
            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }
}
```

The framework validates every returned path against the invocation workspace, publishes each bounded
file, and cleans the workspace on completion, failure, or cancellation. These examples explicitly
offload blocking file work; a reactive event-loop thread must not perform `Files` or archive I/O.

## Publish DSL

Declare the target at top level, then bind terminal pipeline output to it.

```yaml
publish:
  csv-payment-output-files:
    kind: object
    provider: filesystem
    binding: local-files
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

S3 `location` values select the bucket, prefix, and region; they cannot override the S3 endpoint. This prevents a pipeline mapping or runtime request from redirecting framework credentials. Applications that use an S3-compatible endpoint configure an `S3Client` at application bootstrap and register the corresponding source or target provider directly.

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

The built-in CSV Payments replay is the reference connector-first proof. In the captured 1k run,
Object Ingest admitted one source object, the approved and unapproved status paths started before
parser emission finished, `Finalize Payment Output` carried both paths forward, and Object Publish
wrote the terminal output object before success. See [Replay And Live Topology](/operate/observability/replay#csv-payments-built-in-proof) for the measured timings.

See [Metrics](/operate/observability/metrics), [Await Boundary Operations](/operate/await-boundaries), and [Replay And Live Topology](/operate/observability/replay).

## Example Configs

- CSV Payments connector-owned input/output path: `examples/csv-payments/config/pipeline.yaml`
- Search S3 text ingest: `examples/search/config/pipeline.s3-object-ingest.yaml`

See [Field Materialization](/design/materialization) for related claim-check payload representation.
