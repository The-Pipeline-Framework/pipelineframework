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

## Portable Payload Resolution

`payload_ref` has one semantic contract: it identifies immutable payload bytes and carries their
content type, codec, checksum, size, version, and provider metadata. The owner of those bytes may
be either a repository provider or an object-source connector binding.

Consumers resolve both forms through the framework-neutral `PayloadMaterializer`. Materialization
returns an immutable `MaterializedPayload` containing bounded bytes; it does not expose repository results,
filesystem paths, S3 request types, streams, or provider clients. The materializer must reject a
declared or actual payload larger than `maxBytes`, verify integrity when the reference supplies a
checksum, and close all provider resources before its completion stage settles.

Repository-owned references have no connector origin and continue through `RepositoryManager`,
which adapts its internal `RepositoryReadResult` to `MaterializedPayload`. Connector-owned
references carry a `ConnectorPayloadOrigin` and resolve through the exact configured
`ObjectSourceOperation` that produced them. This means field materialization and Object Ingest use
the same reference value and neutral consumption seam while retaining different ownership and
lifecycle paths.

An ordinary step may opt both of its single-`payload_ref` record types into the `file`
representation. Its generated facade uses this same `PayloadMaterializer`, stages the bytes in an
invocation-scoped workspace, and publishes returned files through an existing Object Publish target.
The canonical value before and after the step remains `PayloadReference`; `Path` exists only inside
the authored service boundary.

### Portability and provenance

A connector-owned reference is portable between steps, retries, runtime instances, and releases
only while the destination has a binding with the same binding name, provider and object-source
operation identity, provider major version, and sanitized configuration snapshot. The snapshot
includes non-secret resource configuration and secret-reference names, but never resolved secret
values. Credential rotation is therefore allowed. Changing a resource identity held in binding
configuration, such as a bucket, endpoint, or account, is rejected. Operation locators carried by
the reference itself remain part of that immutable reference instead; version and checksum checks
prevent the locator from silently yielding different content. The current filesystem source uses
this latter form because its root is source-operation input rather than connector-binding
configuration.

Connector-owned references must carry this provenance because release and execution identity do
not pin connector binding definitions. Resolution must compare the captured provenance with the
active binding and fail before provider access when they differ.

The Java record gains a connector-origin component and is therefore a source/binary shape change.
For JSON, old documents that omit `connectorOrigin` deserialize as repository-owned references;
new connector-owned documents serialize that field. For protobuf, the existing fields retain
numbers 1 through 9 and `connector_origin` is additive field 10, so old wire payloads decode with
no connector origin and new readers preserve the old fields.

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
