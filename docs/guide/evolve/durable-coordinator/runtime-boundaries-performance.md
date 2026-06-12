# Runtime Boundaries And Performance

The release runtime cutover adds coordinator-side concepts, but it does not make every execution cross a new network or artifact boundary. The hot path remains transition dispatch through the selected worker.

## Runtime Mapping

Runtime layout and worker selection stay orthogonal.

| Shape | Worker selection | Intended use |
| --- | --- | --- |
| One-process monolith | no remote worker target, so the local in-process worker is selected | local development, demos, first self-host proof |
| Coordinator + REST/gRPC worker | `pipeline.orchestrator.worker.rest.base-url` or `pipeline.orchestrator.worker.grpc.endpoint` configured | production-ish self-host control/data-plane separation |
| Coordinator + SQS worker | `pipeline.orchestrator.worker.sqs.request-queue-url` configured | brokered worker boundary where request/reply queues fit |

The coordinator does not infer worker selection from `pipeline.platform`, `runtimeLayout`, or `monolith`. A monolith can still run the local worker for batteries-included demos. A separated deployment should configure a remote worker target explicitly.

Operators that want to prevent accidental local-worker fallback can enable:

```properties
pipeline.orchestrator.control-plane.require-remote-worker=true
```

When the control-plane API is enabled and this flag is true, startup fails unless exactly one REST, gRPC, or SQS worker target is configured.

## Patterns In Play

The release runtime uses a small set of explicit patterns:

| Pattern | Runtime role |
| --- | --- |
| Release registry | stores release records and activation history per tenant and pipeline |
| Activation and pinning | new executions use the active release; existing executions keep their recorded release identity |
| Artifact descriptor | records immutable artifact identity without making the coordinator a deployer |
| Capability gate | verifies the selected worker reports compatible pipeline, contract, release, and executable identity |
| Control-plane facade | keeps submission, status, await, result, lease, retry, and DLQ ownership on the coordinator side |
| Worker protocol adapter | local, REST, gRPC, and SQS workers execute the same portable transition envelope contract |

The old `bundle-manifest.json` remains the executable-worker identity file for current local/JAR workers. The admin concept is now the active release.

## Performance Posture

The relevant cost boundary is not class count. It is where work runs.

| Path | Work performed | Performance expectation |
| --- | --- | --- |
| Admin registration | parse release descriptor, inspect local/JAR artifact, compute checksum, copy managed artifact | off hot path; can perform blocking file work |
| Activation | update active release pointer and validate stored artifact identity | operator path; not per transition |
| Hosted submit | read active release, verify stored artifact metadata, check worker capability, create execution | one-time admission cost per execution |
| Transition dispatch | claim lease, build pinned transition envelope, invoke selected worker, commit outcome | hot path; must not hash artifacts or scan registries |
| Await resume | use release identity pinned on the execution record | hot path; independent of the currently active release |

Guardrail: artifact hashing and full registry scans must stay out of transition execution. Transition envelopes carry recorded identity; they do not re-validate the release descriptor on every step.

The current submit path does an availability check before accepting hosted work. That is intentional admission work. If this becomes too expensive, the next optimization is caching worker capabilities with a short TTL, not moving validation into transitions.

## Package Boundaries

The runtime package is split by responsibility:

| Package | Responsibility |
| --- | --- |
| `org.pipelineframework.orchestrator` | execution records, stores, queue-async coordinator, transition envelopes, protocol clients, and existing config |
| `org.pipelineframework.orchestrator.release` | contract/release descriptors, release registry, release registrar, and release admin resource |
| `org.pipelineframework.orchestrator.worker` | worker capability and availability checks |

This split is intentionally bounded. It does not move the transition worker protocols or durable execution records because those remain core queue-async runtime concerns.

## Current Limits

1. The one-process monolith remains valid for local/dev proof, not as the recommended separated deployment.
2. Worker lifecycle, heartbeat, drain, and stale-worker state are still outside this slice.
3. File-backed release registry is still local/single-coordinator oriented; Dynamo release metadata is the HA path.
4. Runtime worker-reported artifact digest drift remains a follow-up; current capability checks cover pipeline, contract, release, and executable bundle identity.
