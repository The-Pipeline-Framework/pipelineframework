---
search: false
---

# Field Materialization

Field materialization lets TPF keep large payload fields out of line while preserving the semantic message value. It is representation-level behavior: it is aspect-like in configuration and lifecycle, but it is not an ordinary plugin side effect because it may swap an inline field for a `payload_ref` sibling field.

Use it for claim-check style payloads such as parsed document text, byte blobs, large JSON fragments, or future protobuf/domain payloads that should not be carried through every runtime boundary.

## Message Contract

Mark only fields that are safe to externalize. The inline field keeps its normal semantic type, and the reference is an explicit sibling field so wire compatibility stays visible.

```yaml
messages:
  ParsedDocument:
    fields:
      - number: 1
        name: docId
        type: string
      - number: 2
        name: text
        type: string
        optional: true
        referenceable:
          refField: textRef
      - number: 3
        name: textRef
        type: payload_ref
        optional: true
```

V1 materialization supports scalar `string` and `bytes` fields. Repeated fields, map fields, and nested paths are intentionally deferred.

## Materialization Policy

Policies live under `materialization.aspects`, not business `steps` and not runtime mapping. This keeps the pipeline topology stable while allowing storage cost and replay policy to change.

```yaml
materialization:
  aspects:
    - name: parsed-text-claim-check
      enabled: true
      scope: STEPS
      position: AFTER_STEP
      targetSteps: [Parse Document]
      action: reference
      message: ParsedDocument
      fields: [text]

    - name: chunker-needs-text
      enabled: true
      scope: STEPS
      position: BEFORE_STEP
      targetSteps: [Chunk Document]
      action: dereference
      message: ParsedDocument
      fields: [text]
```

`reference` stores the field in the configured repository provider, clears the inline value, and writes the sibling `payload_ref`. `dereference` loads the payload when the inline value is absent and the reference field is present.

## Repository Providers

Add the repository provider dependency where materialization runs, then select a provider with runtime configuration.

```xml
<dependency>
    <groupId>org.pipelineframework</groupId>
    <artifactId>repository-plugin</artifactId>
    <version>${pipelineframework.version}</version>
</dependency>
```

For Gradle builds, add the equivalent `org.pipelineframework:repository-plugin:${pipelineframeworkVersion}` dependency.

```properties
pipeline.repository.provider=filesystem
pipeline.repository.filesystem.root=target/tpf-repository
pipeline.repository.verify-checksum=true
```

For S3-compatible object storage:

```properties
pipeline.repository.provider=s3
pipeline.repository.s3.bucket=my-pipeline-payloads
pipeline.repository.s3.prefix=dev/search/
pipeline.repository.s3.region=eu-west-1
pipeline.repository.verify-checksum=true
```

Use `pipeline.repository.s3.endpoint-override` and `pipeline.repository.s3.path-style=true` for LocalStack or MinIO.

## Validation

The compiler-facing YAML loader validates these rules early:

- `referenceable.refField` must point to an existing sibling field.
- The sibling field must be optional and typed as `payload_ref`.
- Materialized fields must be scalar `string` or `bytes` in this first slice.
- Materialization policies must name existing messages, fields, positions, actions, and target steps.

Ordinary aspects remain side-effect observations. Field materialization is a framework-owned representation transition that should be transparent to business operators when the policy says the operator receives hydrated data.
