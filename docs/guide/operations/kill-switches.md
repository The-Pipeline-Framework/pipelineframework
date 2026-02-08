# In-flight Probe

The in-flight probe is a guardrail that **detects** when a pipeline run is heading toward resource exhaustion. It
behaves like a liveness probe: it monitors health signals and, when a failure mode is detected, it **signals**
termination rather than allowing the run to continue unchecked.

The termination mechanism is separate: the probe triggers it, and the runtime enforces it by aborting the run.

These probes are intentionally **off by default**. They are safety controls, not general-purpose tuning knobs.

## Retry Amplification Guard

This guard detects **sustained in-flight growth** across the entire pipeline. It is designed for expansion-heavy pipelines
where upstream pace can exceed downstream capacity (for example, CSV readers feeding a slow third-party API).

### What It Measures

The guard samples global inflight counts at a fixed interval (derived from the window), computes the **slope** over the
window, and triggers if that slope exceeds a threshold for a configured number of consecutive samples.

**In-flight items** means the number of items currently being processed or buffered across all steps (summed globally).
This is the same signal you see in the `tpf.step.inflight` metric (aggregated for the pipeline).

In practical terms: it looks for **runaway inflight growth** that persists, not momentary spikes.

### When It Triggers

The guard triggers when:

```text
inflight_slope > inflight_slope_threshold
for sustain-samples consecutive samples
```

Where:
- `inflight_slope` is measured in items/sec
- `sustain-samples` is the number of consecutive samples required
- the evaluation window controls the smoothing period

### Configuration

```properties
pipeline.kill-switch.retry-amplification.enabled=true
pipeline.kill-switch.retry-amplification.window=PT30S
pipeline.kill-switch.retry-amplification.inflight-slope-threshold=1.0
pipeline.kill-switch.retry-amplification.sustain-samples=3
pipeline.kill-switch.retry-amplification.mode=fail-fast
```

### Telemetry

When triggered, the run span records:
- `tpf.kill_switch.triggered=true`
- `tpf.kill_switch.reason=retry_amplification`
- `tpf.kill_switch.step=global`
- `tpf.kill_switch.inflight_slope`
- `tpf.kill_switch.inflight_slope_threshold`
- `tpf.kill_switch.sustain_samples`

Metric:
- `tpf.pipeline.kill_switch.triggered` increments

### How it compares to Kubernetes liveness

The intent is similar to a Kubernetes liveness probe: the probe detects an unhealthy condition, and a separate
termination mechanism restarts or aborts the workload. In TPF, the probe detects sustained in-flight growth and
signals the runtime to abort the current run before resources are exhausted.

### Tuning Guidance

**Expansion steps** (1→N) tend to create slow, steady inflight growth. Tune for those:

- Start with `window=PT30S` and `sustain-samples=3`
- Lower the slope threshold until it trips at the point you consider unhealthy
  (for example, if inflight grows by +1000 every 5 minutes, slope is ~3.33/sec)

**Fast bursts** can create short spikes; the sustain-samples requirement is the primary protection against false positives.

### Demo-Friendly Settings

For a short demo that should trigger quickly when inflight growth starts:

```properties
pipeline.kill-switch.retry-amplification.window=PT30S
pipeline.kill-switch.retry-amplification.inflight-slope-threshold=1.0
pipeline.kill-switch.retry-amplification.sustain-samples=3
pipeline.kill-switch.retry-amplification.mode=fail-fast
```

## Fail-Fast vs Log-Only

- `fail-fast`: throws a runtime exception and aborts the run
- `log-only`: records telemetry but continues execution

Use `log-only` in staging if you want to validate thresholds before enforcing them.
