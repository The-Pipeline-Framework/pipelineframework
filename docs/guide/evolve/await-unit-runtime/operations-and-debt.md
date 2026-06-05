# Await Unit Limitations And Debt

This implementation-facing page tracks limitations and follow-up work for the await unit model.

Application-facing guidance lives in [Await Boundaries](/guide/development/orchestrator-runtime/await). Operational guidance lives in [Await Boundary Operations](/guide/operations/await-boundaries).

## Limitations

1. Await requires `QUEUE_ASYNC`.
2. External dispatch and external side effects remain at-least-once.
3. Aggregate await units materialize input and/or output in v1. Runtime item-count guards now bound materialized input and output units by default, but architects should still avoid unbounded aggregate payloads.
4. Replay restarts a materialized output unit as a whole; there is no exactly-once partial progress inside the unit.
5. Transport adapters have different operational obligations: `interaction-api` needs an API consumer, `webhook` needs signed token configuration, and `kafka` needs broker channels and consumer health.
