# Concurrency and Backpressure Sizing

Concurrency and backpressure settings are the two levers that determine how well the pipeline can keep doing useful
work while waiting for I/O.

### How to size `pipeline.max-concurrency`

`pipeline.max-concurrency` limits live work admitted by a step or live connector segment. For brokered `ONE_TO_ONE` await over a stream, it also acts as the pending-interaction window: the parser can dispatch up to that many await interactions, then advances as completions are durably recorded and accepted by downstream demand.

1. **Start from CPU cores and I/O profile**:
   - CPU-bound steps: set concurrency near the number of vCPUs (for example 4 cores → 4–8).
   - I/O-bound steps: you can go higher (for example 4 cores → 32–128), but validate with metrics.
2. **Watch `tpf.step.inflight`**:
   - If it stays far below the limit, the step cannot use the concurrency (increase only if you see queueing).
   - If it is pinned at the limit and `tpf.step.buffer.queued` is growing, you need more concurrency or a faster downstream.

### How to size `pipeline.defaults.backpressure-buffer-capacity`

1. **Size for burst absorption**:
   - Buffer capacity should cover a burst window you are willing to absorb, not the entire dataset.
2. **Estimate memory impact**:
   - `buffer capacity × average item size = memory footprint per step`.
   - Example: 4-core pod, 1 KB average item size, buffer 4,000 → ~4 MB per step (plus overhead).
3. **Use the buffer metrics**:
   - `tpf.step.buffer.queued` should spike and drain.
   - Flat, high values indicate backpressure is not propagating or downstream is too slow.

### Practical starting point (example)

For a 4-core Graviton pod running I/O-heavy steps:
- `pipeline.max-concurrency`: start at 32–64 for I/O-heavy steps, 4–8 for CPU-heavy steps.
- `pipeline.defaults.backpressure-buffer-capacity`: start at 2,048–8,192 for bursty streams and tune downward.

Always validate in your environment using the in-flight and buffer metrics and adjust per step when needed.

### Quick rule-of-thumb table

| Workload profile | `pipeline.max-concurrency` (per step) | Buffer capacity guidance |
|------------------|---------------------------------------|--------------------------|
| CPU-bound        | 1–2 × vCPU cores                       | Small (128–1,024)        |
| Mixed            | 4–16 × vCPU cores                      | Medium (1,024–4,096)     |
| I/O-bound        | 8–32 × vCPU cores                      | Larger (2,048–8,192)     |

Tune downward if the buffer stays high or GC increases, and upward only when `tpf.step.inflight` is consistently
below the limit while `tpf.step.buffer.queued` spikes.

### Durable boundaries

Backpressure propagates through live reactive segments. A brokered await can participate in that live flow while the same queue-async transition still owns a live await session: source dispatch is bounded by `pipeline.max-concurrency`, completions are recorded durably, and downstream demand decides when accepted completions move into the next step.

Durability takes over when the live session is unavailable. A request may complete later through Kafka, SQS, a webhook, a human/API completion, or a restarted worker. In that path, TPF uses await unit state, execution state, and queue admission rather than a single in-memory demand signal.

For await-heavy pipelines, size the system around two kinds of pressure:

- live segment pressure: step inflight counts, step buffers, pending live await interactions, terminal publish write latency, and source admission;
- durable boundary pressure: pending await interactions, completions waiting for durable fallback continuation, work-queue depth, provider permits, broker lag, retry rate, and DLQ events.

In connector-first CSV Payments, Object Ingest controls source-object admission, the CSV parser advances by reactive demand and the live await in-flight window, and Object Publish accepts terminal chunks through a target session. The old CSV reader demand pacer is a legacy fallback for the deprecated file-step path; it is not the main backpressure mechanism for the connector-owned path.

```mermaid
flowchart LR
    A["Object Ingest<br/>source object"] --> B["CSV parser<br/>demand-driven iterator"]
    B --> C["Await Payment Provider<br/>max in-flight interactions"]
    C --> D["Kafka/provider<br/>external latency"]
    D --> E["Live await session<br/>durable completion first"]
    E --> F["Process Payment Status"]
    F --> G["Object Publish<br/>streaming target session"]
    C -. "durable fallback" .-> H["WAITING_EXTERNAL<br/>item continuations"]
    H -. "restart or no live session" .-> F
```

### Retry amplification example (real-world)

When an upstream source can admit work faster than a downstream step can call a slow third-party
(avg 250 ms per item), it is easy to misconfigure concurrency and trigger retry amplification.

Observed pattern:
- Retries climb while `tpf.step.inflight` on the third-party step grows steadily (for example +1,000 every 5 minutes).
- The input step buffer utilization stays below 80%.
- Success rate oscillates around 50% as retries saturate the step.

Mitigations:
- Lower per-step concurrency on the third-party step.
- Increase retry wait/backoff to reduce retry pressure.
- Align source admission, per-step concurrency, and downstream throughput.
- Consider enabling the retry amplification guard in `log-only` mode first, then switch to `fail-fast`
  once you have stable thresholds for inflight slope and retry rate.

```mermaid
flowchart LR
    A[Object Ingest<br/>Source Admission] -->|items| B[Input Step Buffer]
    B --> C[Third-party Step<br/>Throttle + Retries]
    C --> D[Third-party API]
    C -. retry amplification .-> C
    B -. buffer below 80% .- B
    C -. in-flight grows .- C
```

## Deployment

- Package services as independent deployable units
- Use containerization for consistent deployment
- Configure health checks and readiness probes
