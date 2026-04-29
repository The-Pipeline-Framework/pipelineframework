---
search: false
---

# Operator Troubleshooting Matrix

Use this matrix to map failure signatures to likely root causes and first actions.

Build and CI failures are covered in the development guide:
- [Operator Build Troubleshooting](/versions/v26.4.5/guide/development/operators-build-troubleshooting)

## Runtime Signatures

| Signature (exact/close match) | Likely Cause | First Actions |
| --- | --- | --- |
| Invalid invocation mode (`IllegalArgumentException`) | Misconfigured/unsupported `tpf.function.invocation.mode` value | Verify configured `tpf.function.invocation.mode`, correct to a supported value, redeploy, classify affected payloads and replay if needed; add logs/metrics to capture bad config inputs |
| Retry exhaustion (transient retries consumed) | Downstream dependency instability or persistent transient condition | Stabilise downstream service, then replay parked/failed workload |
| Parking lot growth / repeated parked failures | Non-retryable payloads or repeated hard dependency failures | Classify by error type and payload pattern; route bad payloads and fix dependency |
| Non-deterministic lineage or merge-order drift across retries/replays | Unstable item ordering keys (payload identity-based ordering, missing tie-breakers) | Re-run slice-1 runtime tests, verify deterministic fingerprint/ordering keys, and avoid identity-only ordering inputs |
| Timeout/latency spikes on operator-heavy step | Operator compute cost increase (that is, steps running many stateful operators or CPU/GPU-intensive transforms per payload), saturation, or downstream pressure | Correlate with payload/traffic changes, scale or throttle, then tune retry/backoff |
| Handler not selected / wrong lambda entrypoint | Multiple generated handlers with ambiguous runtime selection | Set explicit `quarkus.lambda.handler` for target module |
| Missing generated resource/handler classes | Build target mismatch or generation path drift (that is, build output moved/renamed due to changed generation/build settings) | Re-run CI-equivalent compile lane with expected platform/transport flags |
| `StepManyToOne.applyReduce` compile mismatch in generated client step | Framework/client-step renderer drift for MANY_TO_ONE shape | Rebuild framework deployment module and regenerate sources; confirm generated client method is `applyReduce(Multi<...>)` |
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

- [Operator Runbook](/versions/v26.4.5/guide/operations/operators-playbook)
- [Operators](/versions/v26.4.5/guide/build/operators)
