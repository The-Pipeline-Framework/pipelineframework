# Await Unit Operations And Debt

This page covers runtime boundaries, operational limitations, and tracked follow-up work for the await unit model.

## Await Versus Checkpoint Handoff

Await and checkpoint handoff both cross a process boundary, but they solve different problems.

| Concern | Await unit | Checkpoint handoff |
| --- | --- | --- |
| Ownership | one execution parks and later resumes | source pipeline publishes; target pipeline owns its own execution |
| Boundary | mid-pipeline external wait | terminal or named publication boundary |
| Resume | same execution continues from `awaitUnitId` | downstream execution starts/admits work independently |
| State of record | await unit + interaction stores | execution state + checkpoint publication/admission |
| DLQ responsibility | owning execution remains responsible | downstream orchestrator owns retry/DLQ after admission |
| Use when | external result belongs to the same business flow | another pipeline should own the next lifecycle |

Do not use await to simulate a second pipeline. If the receiving workflow has separate ownership, lifecycle, scaling, or DLQ responsibility, use checkpoint handoff.

Do not use checkpoint handoff to model a human approval, webhook callback, or brokered provider decision that must resume the same execution. That is an await unit.

## Limitations

1. Await requires `QUEUE_ASYNC`.
2. External dispatch and external side effects remain at-least-once.
3. Aggregate await units materialize input and/or output in v1. Runtime item-count guards now bound materialized input and output units by default, but architects should still avoid unbounded aggregate payloads.
4. Replay restarts a materialized output unit as a whole; there is no exactly-once partial progress inside the unit.
5. Transport adapters have different operational obligations: `interaction-api` needs an API consumer, `webhook` needs signed token configuration, and `kafka` needs broker channels and consumer health.

## Tracked Debt

1. [#304](https://github.com/The-Pipeline-Framework/pipelineframework/issues/304): generated common modules should not trigger `com.google.protobuf` split-package warnings.
2. [#305](https://github.com/The-Pipeline-Framework/pipelineframework/issues/305): template generator should scaffold union DTO/mappers for REST await outputs.
3. [#311](https://github.com/The-Pipeline-Framework/pipelineframework/issues/311): template generator should not emit confusing inactive runtime mapping variants.
