---
search: false
---

# Runtime Efficiency

<p class="value-lead">TPF is tuned for high-volume Java function flows where throughput and stability matter day to day.</p>

## At a Glance

<div class="value-glance">
  <div class="value-glance-item"><strong>High Throughput</strong> &middot; Reactive flow keeps many items moving without one thread per item.</div>
  <div class="value-glance-item"><strong>Predictable Runtime</strong> &middot; Generated function calls and runtime files reduce hidden glue code.</div>
  <div class="value-glance-item"><strong>Quarkus Foundation</strong> &middot; Keep Quarkus startup, container, and runtime strengths.</div>
</div>

## Use This When

- Infrastructure cost is rising as traffic grows.
- You need faster startup and tighter runtime footprint.
- Performance tuning is blocked by opaque framework behaviour.

Reactive execution means the runtime can keep work moving while waiting for I/O, instead of tying up a thread for each item from start to finish.
If a step must do blocking work, offload it explicitly to a worker thread pool, executor, or blocking dispatcher so it does not occupy request/event-loop threads.

YAML-first generation also helps performance work stay concrete: the generated flow, callers, and runtime files make it easier to see where work enters, how it moves, and which function should be tuned.

## Jump to Guides

<div class="value-links">

- [Performance](/versions/v26.6.2/develop/performance)
- [Pipeline Compilation](/versions/v26.6.2/develop/pipeline-compilation/)
- [Architecture](/versions/v26.6.2/evolve/architecture)

</div>
