# Representations and runtime mappings

Read this reference only when canonical values meet Java code, persistence, files,
object connectors, provider DTOs, serialization, or remote boundaries. Verify each
consumer's current support in source/docs/tests; a declared mapping is not a promise
that every consumer can use it.

## Keep four identities visible

```text
canonical pipeline value
    != necessarily authored Java representation
    != necessarily persistence representation
    != necessarily wire representation
```

Concrete example: YAML carries a `SourceDocument` record containing `payload_ref`.
An authored transform can receive `Path`. Persistence can store a typed JPA entity.
A remote Await can send generated protobuf. Those are four realizations of the same
semantic flow, and none should be leaked into the others merely to avoid a mapper.

| Surface | What it means | What it does not mean |
| --- | --- | --- |
| canonical `types:` | portable business meaning, applicability, durable contract | Java SDK shape, table schema, or wire encoding |
| step `java:` | Java execution binding/assertion visible to this compiling module | conversion, persistence identity, or wire identity |
| type `mappings:` | named external representation available to a specific consumer | a globally selectable serialization mode |
| representation provider | validates/describes/generates a supported boundary | application flow engine or provider client registry |
| connector mapper/binding | admission/publication/provider conversion and configuration | canonical business model |
| `pipeline.runtime.yaml` | logical placement of steps and synthetics | value conversion or Maven topology |

For an inspectable local service/operator, let TPF infer Java types. Add `java.input`
or `java.output` to assert inference, resolve ambiguity, or bind a framework-owned,
remote, or cross-module contract. Compilation visibility is module/classpath scoped;
sibling source is not magically visible.

Add a mapper only where representation actually changes. Mapper selection must be
pair-accurate for the domain and external types. An inbound and outbound boundary may
use different external types. Do not add a mapper that merely restates an inspectable
method signature, and do not rely on a generic “close enough” converter.

## Use the representation consumer, not application glue

`mappings.persistence` adapts the canonical boundary value to the persistence
provider's typed representation. A JPA entity is table-facing plumbing, not pipeline
state. Persist the canonical root value observed at the boundary; nested types do not
automatically deserve independent tables. Inspect the current persistence consumer's
supported type shapes and required mapper direction before authoring entities.

The `file` representation is one representation-provider use case, not the definition
of mappings. It can generate a facade around an ordinary `Path -> Path` service while
the pipeline still carries records containing `PayloadReference` and keeps its declared
cardinality. The generated boundary owns bounded materialization, invocation workspace,
publication, cleanup, and conversion back to a canonical reference.

If a provider/library needs OpenCSV rows, media objects, JPA entities, or another DTO,
search the representation-provider SPI and installed providers first. Keep genuine
application semantics in authored mappers; let providers generate deterministic
boundary machinery. If the same boilerplate recurs because support is missing, report
an ergonomics/framework gap rather than changing the canonical model.

## Large content and object boundaries

`PayloadReference` is the canonical durable value for large immutable/object content.
It carries stable reference/provenance/integrity metadata. `Path`, byte arrays, streams,
and media objects are bounded local representations with invocation/runtime lifetimes.

Object Ingest owns polling/admission, snapshot identity, source binding, materialization,
and the first typed projection. The service should receive the canonical input—not an
S3 request, filesystem location, or `ObjectSnapshot`—when the current generated boundary
supports it.

Grouped selection admits several objects as one canonical value. Use named fields for
different roles or one repeated `payload_ref` field for a homogeneous bundle. The bundle
is still one value; choose streaming cardinality separately.

Object Publish owns terminal publication and returned durable references. The authored
step returns a typed value/local representation; it does not inject the target client.
Do not pass storage keys or provider locations through business types when the connector
binding/reference already owns them.

## Durable and wire boundaries

Canonical type names, fields, wrappers, and union discriminators define semantic
contract identity. Representation mappings are application/build configuration and do
not allocate protobuf tags or redefine wire compatibility. Generated transport adapters
convert immediately at the boundary; protobuf/JSON/provider envelopes should not become
resumed business values.

What crosses a remote/durable boundary depends on that boundary's generated contract.
A local `Path`, stream, provider instance, secret, connection, session, or runtime handle
does not survive it. Carry the canonical value or durable reference and let the remote
runtime materialize its own local form. Inspect the generated contract/metadata and the
focused codec/adapter tests before assuming a Java object is portable.

Search order: template DSL external-representation and Java-binding sections; current
consumer docs; representation-provider API and generation phases; mapper inference;
`PayloadReference`/materializer/object connector code; then generated-boundary and
round-trip/size/integrity/cleanup tests.
