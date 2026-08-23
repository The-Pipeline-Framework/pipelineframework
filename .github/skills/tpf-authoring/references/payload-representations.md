# Payload representations

Read this reference only for large content, files/media, object ingest/publish, materialization, or representation work. Verify exact mappings and provider support in current docs/source/tests.

`PayloadReference` is the canonical durable value for large immutable/object data. It carries stable identity and the current implementation's provenance, integrity, size/type, and origin metadata.

Bytes, streams, media objects, and `Path` are local representations. Prefer a current representation provider or generated facade that materializes a bounded local form for an authored service and returns to the canonical reference at the pipeline boundary.

For example, an authored file transform may be `Path -> Path` while YAML uses canonical records containing `payload_ref`. The generated boundary owns materialization, invocation workspace, publication, cleanup, and returned `PayloadReference`.

Object Ingest/Publish connectors own admission/publication. The authored service owns the typed transformation. Do not pass repository results, S3 requests, connector clients, storage keys, or materializer internals through business types when a representation seam exists.

Search current materialization/object-ingest authoring docs first, then `PayloadReference`, materializer, representation-provider and connector SPIs, generated boundary code, and size/integrity/cleanup tests.
