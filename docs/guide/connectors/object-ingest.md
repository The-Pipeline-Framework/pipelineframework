# Object Ingest

Object ingest lets TPF admit files or object-store keys into a queue-async pipeline without making a business step list folders, poll S3, dedupe keys, or construct async execution ids.

Use it when an object arrival or object listing is the pipeline input. Business steps should receive a domain input such as `CsvPaymentsInputFile` or `RawDocument`; the object shell owns listing, filtering, payload references, identity, and async admission.

## Pipeline DSL

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

```yaml
steps:
  - name: Process Csv Payments Input
    service: org.pipelineframework.csv.service.ProcessCsvPaymentsInputService
    cardinality: EXPANSION
    input: org.pipelineframework.csv.common.domain.CsvPaymentsInputFile
    inputTypeName: CsvPaymentsInputFile
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

## Connectors

Add the connector library where object ingest runs:

```xml
<dependency>
    <groupId>org.pipelineframework</groupId>
    <artifactId>object-ingest-connector</artifactId>
    <version>${pipelineframework.version}</version>
</dependency>
```

V1 object source connectors:

| Connector | Purpose |
| --- | --- |
| `filesystem` | Local folders, tests, CSV-style batch inputs. |
| `s3` | AWS S3-compatible object listing and text/reference payload admission. |

The YAML field remains `provider` in v1 because it selects the Java `ObjectSourceProvider`
implementation behind the connector. The user-facing category is connectors because these
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

## Runtime Requirements

Object ingest v1 requires `pipeline.orchestrator.mode=QUEUE_ASYNC`. TPF submits each mapped input with a deterministic idempotency key derived from object identity, so duplicate listing results resolve to existing async executions.

FUNCTION pipelines are rejected in v1. Quarkus currently hosts the bootstrap, but the ingest runner and provider SPI are plain Java so a Spring Boot host can wire the same semantics later.

## Example Configs

- CSV Payments folder replacement: `examples/csv-payments/config/pipeline.object-ingest.yaml`
- Search S3 text ingest: `examples/search/config/pipeline.s3-object-ingest.yaml`

See [Field Materialization](/guide/plugins/materialization) for related claim-check payload representation.
