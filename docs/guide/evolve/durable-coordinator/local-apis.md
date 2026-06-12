# Local Control-Plane APIs

The runtime includes default-disabled local/dev APIs that prove coordinator ownership of submission, status/result lookup, await query/completion, release activation, and worker dispatch.

They are internal runtime groundwork and self-host reference surfaces, not a general hosted-service API.

## Control Plane

Enable the generic control-plane resource:

```properties
pipeline.orchestrator.control-plane.enabled=true
pipeline.orchestrator.control-plane.admin-token=local-control-token
```

Main endpoints:

1. `POST /tpf/control-plane/tenants/{tenantId}/executions`
2. `GET /tpf/control-plane/tenants/{tenantId}/executions/{executionId}`
3. `GET /tpf/control-plane/tenants/{tenantId}/executions/{executionId}/result`
4. `GET /tpf/control-plane/tenants/{tenantId}/interactions/pending`
5. `POST /tpf/control-plane/tenants/{tenantId}/interactions/complete`

Requests require `Authorization: Bearer <token>`.

## Release Admin

Enable the local release admin resource:

```properties
pipeline.orchestrator.admin.enabled=true
pipeline.orchestrator.admin.admin-token=local-admin-token
pipeline.orchestrator.bundles.registry.provider=file
pipeline.orchestrator.bundles.storage.root=target/tpf-bundles
```

Use `pipeline.orchestrator.bundles.registry.provider=dynamo` plus `pipeline.orchestrator.dynamo.release-table` when the release metadata must survive multiple coordinator instances. The Dynamo registry stores immutable release records and append-only activation events.

Main endpoints:

1. `POST /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/releases/register`
2. `GET /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/releases`
3. `GET /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/releases/active`
4. `GET /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/releases/{releaseVersion}`
5. `POST /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/releases/{releaseVersion}/activate`

The registration API accepts an absolute local `pipeline-release.json` path. The release descriptor pins one or more artifacts by kind and digest. For executable local JAR artifacts, the registrar validates `META-INF/pipeline/bundle-manifest.json`, copies the JAR into the local store, records size/checksum metadata, and stores the managed artifact path.

The older `/bundles` endpoints remain local compatibility helpers. New self-host flows should register and activate releases.
