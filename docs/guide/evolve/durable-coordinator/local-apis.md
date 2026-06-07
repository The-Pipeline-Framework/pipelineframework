# Local Control-Plane APIs

The runtime includes default-disabled local/dev APIs that prove coordinator ownership of submission, status/result lookup, await query/completion, bundle activation, and worker dispatch.

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

## Bundle Admin

Enable the local bundle admin resource:

```properties
pipeline.orchestrator.admin.enabled=true
pipeline.orchestrator.admin.admin-token=local-admin-token
pipeline.orchestrator.bundles.registry.provider=file
pipeline.orchestrator.bundles.storage.root=target/tpf-bundles
```

Main endpoints:

1. `POST /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/bundles/register`
2. `GET /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/bundles`
3. `GET /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/bundles/active`
4. `GET /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/bundles/{bundleVersionId}`
5. `POST /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/bundles/{bundleVersionId}/activate`

The current registration API accepts an absolute local artifact path. The registrar validates the JAR manifest, copies the artifact into the local store, records size/checksum metadata, and stores the managed artifact path.
