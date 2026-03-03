# Operator Troubleshooting Matrix

Use this matrix to map failure signatures to likely root causes and first actions.

Build and CI failures are covered in the development guide:
- [Operator Build Troubleshooting](/guide/development/operators-build-troubleshooting)

## Runtime Signatures

| Signature (exact/close match) | Likely Cause | First Actions |
| --- | --- | --- |
| `IllegalArgumentException` mentioning function invocation mode | Invalid `tpf.function.invocation.mode` value (no silent fallback) | Validate configured mode against supported values, correct config, and rerun the lane |
| Retry exhaustion (transient retries consumed) | Downstream dependency instability or persistent transient condition | Stabilise downstream service, then replay parked/failed workload |
| Parking lot growth / repeated parked failures | Non-retryable payloads or repeated hard dependency failures | Classify by error type and payload pattern; route bad payloads and fix dependency |
| Timeout/latency spikes on operator-heavy step | Operator compute cost increase (that is, steps running many stateful operators or CPU/GPU-intensive transforms per payload), saturation, or downstream pressure | Correlate with payload/traffic changes, scale or throttle, then tune retry/backoff |
| Handler not selected / wrong lambda entrypoint | Multiple generated handlers with ambiguous runtime selection | Set explicit `quarkus.lambda.handler` for target module |
| Missing generated resource/handler classes | Build target mismatch or generation path drift (that is, build output moved/renamed due to changed generation/build settings) | Re-run CI-equivalent compile lane with expected platform/transport flags |
## Diagnostics Workflow

1. Confirm commit and artifact match the tested revision.
2. Reproduce with CI-equivalent command for the lane.
3. Match error text to the matrix row.
4. Apply first action and rerun the narrowest relevant test lane.
5. Expand to full verify only after the targeted lane is green.

## Quick Validation Commands

```bash
./mvnw verify
./mvnw -f framework/pom.xml verify
```

## Related

- [Operator Runbook](/guide/operations/operators-playbook)
- [Operators](/guide/build/operators)
