---
search: false
---

# Local Control-Plane APIs

The runtime includes default-disabled local/dev APIs that prove coordinator ownership of submission, status/result lookup, await query/completion, release activation, and worker dispatch.

They are internal runtime groundwork and self-host reference surfaces, not a general hosted-service API.

For how these APIs relate to coordinator and worker processes, see [Coordinator And Worker Topology](/versions/v26.6.1/guide/evolve/durable-coordinator/coordinator-worker-topology).

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
pipeline.orchestrator.releases.registry.provider=file
pipeline.orchestrator.releases.storage.provider=local
pipeline.orchestrator.releases.storage.root=target/tpf-releases
```

Use `pipeline.orchestrator.releases.registry.provider=dynamo` plus `pipeline.orchestrator.dynamo.release-table` when the release metadata must survive multiple coordinator instances. Pair it with `pipeline.orchestrator.releases.storage.provider=s3` only for artifacts that should be coordinator-managed blobs. Container images should stay in OCI registries and be referenced by digest. The Dynamo registry stores immutable release records and append-only activation events.

Main endpoints:

1. `POST /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/releases/register`
2. `GET /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/releases`
3. `GET /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/releases/active`
4. `GET /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/releases/{releaseVersion}`
5. `POST /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/releases/{releaseVersion}/activate`

The registration API accepts an absolute local `pipeline-release.json` path. The release descriptor pins one or more artifacts by kind and digest. For executable local artifacts, the registrar validates embedded `META-INF/pipeline/pipeline-contract.json` when present, copies the artifact into the configured release artifact store, records size/checksum metadata, and stores the managed artifact URI. For container-image artifacts, registration records the immutable OCI reference; platform deployment and worker capability checks prove the running worker identity.

## Worker Lifecycle Admin

The same admin surface exposes a minimal worker lifecycle registry:

```properties
pipeline.orchestrator.admin.enabled=true
pipeline.orchestrator.admin.admin-token=local-admin-token
pipeline.orchestrator.worker.lifecycle.provider=memory
pipeline.orchestrator.worker.lifecycle.stale-after=PT2M
```

Use `pipeline.orchestrator.worker.lifecycle.provider=dynamo` plus `pipeline.orchestrator.dynamo.worker-table` when multiple coordinator instances need the same worker lifecycle view. The Dynamo provider stores append-only registration, heartbeat, and drain events.

Main endpoints:

1. `POST /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/workers/register`
2. `POST /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/workers/{workerId}/heartbeat`
3. `POST /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/workers/{workerId}/drain`
4. `GET /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/workers`

Worker registration records the worker id, protocol, endpoint, contract version, release version, and optional artifact id/digest. Hosted execution submission requires at least one matching `HEALTHY` worker lifecycle record after the selected worker capability check succeeds. `STALE`, `DRAINING`, and `UNAVAILABLE` workers do not admit new hosted executions.
