# Operator Troubleshooting Matrix

Use this matrix to map failure signatures to likely root causes and first actions.

## Build and CI Signatures

| Signature (exact/close match) | Likely Cause | First Actions |
| --- | --- | --- |
| `Class not found` / `method not found` during operator resolution | Operator class not on build classpath, wrong FQCN, or wrong method name | Verify dependency graph, package name, and `operator: Class::method` value in YAML |
| `More than 1 matching method` / overloaded method ambiguity | Operator method overloading unsupported for resolved path | Rename target method or make operator entrypoint unique |
| `Method has more than 1 parameter` | Operator contract mismatch with invoker expectations | Refactor to single input parameter (or unary input shape) |
| `gRPC transport requires protobuf descriptors` | Descriptor set unavailable for gRPC delegated/operator path | Verify descriptor generation and build inputs for gRPC modules |
| `... requires a mapper ...` for gRPC delegated/operator step | Missing or non-matching mapper binding for transport path | Add/fix mapper pair and ensure binding generation matches routing conventions |
| `Build step ... does not produce any build item` | Quarkus build step wiring issue | Check `@BuildStep` producer/consumer contract and `@Produce` usage |

## Runtime Signatures

| Signature (exact/close match) | Likely Cause | First Actions |
| --- | --- | --- |
| Retry exhaustion (transient retries consumed) | Downstream dependency instability or persistent transient condition | Stabilise downstream service, then replay parked/failed workload |
| Parking lot growth / repeated parked failures | Non-retryable payloads or repeated hard dependency failures | Classify by error type and payload pattern; route bad payloads and fix dependency |
| Timeout/latency spikes on operator-heavy step | Operator compute cost increase, saturation, or downstream pressure | Correlate with payload/traffic changes, scale or throttle, then tune retry/backoff |
| Handler not selected / wrong lambda entrypoint | Multiple generated handlers with ambiguous runtime selection | Set explicit `quarkus.lambda.handler` for target module |
| Missing generated resource/handler classes | Build target mismatch or generation path drift | Re-run CI-equivalent compile lane with expected platform/transport flags |

## Diagnostics Workflow

1. Confirm commit and artifact match the tested revision.
2. Reproduce with CI-equivalent command for the lane.
3. Match error text to the matrix row.
4. Apply first action and rerun the narrowest relevant test lane.
5. Expand to full verify only after the targeted lane is green.

## Quick Validation Commands

```bash
./mvnw -f framework/pom.xml verify

./mvnw -f examples/search/pom.xml -pl orchestrator-svc -am \
  -Dpipeline.platform=FUNCTION \
  -Dpipeline.transport=REST \
  -Dpipeline.rest.naming.strategy=RESOURCEFUL \
  -DskipTests compile
```

## Related

- [Operator Runbook](/guide/operations/operators-playbook)
- [Operators](/guide/build/operators)
