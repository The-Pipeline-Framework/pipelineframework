---
search: false
---

# Scalability through increased concurrency and managed backpressure

Reactive pipelines scale by keeping CPU busy while waiting on I/O. That relies on two levers:

- **Concurrency** (`pipeline.max-concurrency`): how many items a step is allowed to process in parallel.
- **Backpressure buffer capacity** (`pipeline.defaults.backpressure-buffer-capacity`): how many items can be queued
  when upstream is faster than downstream.

These are configured as global defaults but apply **per step**. In other words, every step gets its own concurrency
limit and its own buffer unless you override it per step. This is intentional: each step can have a different I/O
profile and needs separate tuning.

Operationally, you use observability to validate both:
- `tpf.step.inflight` should stay near (but not pinned at) the configured max concurrency for I/O-heavy steps.
- `tpf.step.buffer.queued` should spike during bursts but should not stay flat and high; sustained growth means the
  downstream is too slow or the buffer is too small.

You can also define a canonical "item" type for telemetry:

```properties
pipeline.telemetry.item-input-type=com.example.domain.PaymentRecord
pipeline.telemetry.item-output-type=com.example.domain.PaymentOutput

# The input type maps to the first step that consumes that type.
# The output type maps to the last step that produces that type.
```

TPF maps the input type to the first step that consumes it and the output type to the last step that produces it, then
emits `tpf.item.consumed` and `tpf.item.produced` counters for that boundary.

Aspect position note: AFTER_STEP observes the output of each step. This captures every boundary
except the very first input boundary (before the pipeline starts). Conversely, BEFORE_STEP captures
every boundary except the final output boundary (after the pipeline completes). Use two aspects if
you need complete boundary coverage.

TPF also emits SLO-ready counters (under `tpf.slo.*`) using the thresholds configured via:
`pipeline.telemetry.slo.rpc-latency-ms` and `pipeline.telemetry.slo.item-throughput-per-min`.
