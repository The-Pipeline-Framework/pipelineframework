# Await Unit Limitations And Debt

This implementation-facing page tracks limitations and follow-up work for the await unit model.

Application-facing design guidance lives in [Await Boundaries](/design/await-boundaries). Runtime setup lives in [Await runtime setup](/deploy/orchestrator-runtime/await). Operational guidance lives in [Await Boundary Operations](/operate/await-boundaries).

## Limitations

1. Await requires `QUEUE_ASYNC`.
2. External dispatch and external side effects remain at-least-once.
3. Aggregate await units materialize input and/or output in v1. Runtime item-count guards now bound materialized input and output units by default, but architects should still avoid unbounded aggregate payloads.
4. Replay restarts a materialized output unit as a whole; there is no exactly-once partial progress inside the unit.
5. Await adapters have different operational obligations: `interaction-api` needs an API consumer, `webhook` needs signed token configuration, and Kafka and SQS need broker or queue channels and consumer/poller health.
6. Memory/event providers support the canonical await lifecycle only while one process remains alive. They do not provide local restart recovery; that requires an embedded coordination-store suite for execution, await, command-effect, lease, and admission state.
7. The unchanged self-host 10k CSV acceptance does not yet meet its 180-second worker deadline. #541 tracks the remaining live-path throughput limitation; the 1k replay capture is a behavioral proof, not a scale-performance guarantee.
