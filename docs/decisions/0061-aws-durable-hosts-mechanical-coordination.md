---
title: AWS Durable hosts mechanical queue-async coordination
status: accepted
---

# ADR-0061: AWS Durable hosts mechanical queue-async coordination

## Context

The native `QUEUE_ASYNC` coordinator proves TPF's execution, Await, retry, DLQ, worker, release-pinning, and re-drive semantics. Hosting all of its liveness machinery directly on AWS would also require TPF to own durable suspension, callback wake-up, checkpoint replay, scheduling, and associated recovery races.

A deployed fault proof demonstrates that Lambda Durable Functions can own those mechanics while bounded, idempotent TPF actions and a reconstructable TPF checkpoint retain semantic authority.

## Decision

Use AWS Lambda Durable Functions as the preferred candidate AWS `QUEUE_ASYNC` coordination host.

- AWS Durable owns orchestration liveness, checkpoints, suspension, provider callback wake-up, and mechanical retry of bounded actions.
- TPF owns execution and Await identity, typed completion admission, parent release, release pinning, signed worker transitions, semantic retry and DLQ evidence, results, and operator-authorised re-drive.
- Provider callback identifiers, execution ARNs, history, and callback generations remain hosting correlations outside the provider-neutral semantic checkpoint.
- SQS remains the worker boundary until a separate proof shows direct invocation preserves backpressure, redelivery, DLQ evidence, and uncertain-outcome handling.
- `PipelineControlPlane` remains the action contract. A coordination host invokes it; it does not introduce a parallel execution API.
- The native coordinator remains the portable semantic reference, conformance implementation, and fallback architecture.

The deployed proof is retained as non-published verification. This decision does not itself declare supported production packaging.

## Rationale

Delegating mature checkpoint, suspension, and wake-up mechanics reduces the distributed-systems surface maintained by TPF without creating a second semantic authority. Stable TPF identities and idempotent transitions make provider replay safe, while the TPF checkpoint permits recovery after provider-history loss.

The boundary becomes invalid if callback or provider history must become the only record of a semantic transition, or if recovery requires duplicating the complete TPF state machine in provider history.

## Consequences

- AWS hosting may use provider-specific infrastructure while preserving one portable TPF execution model.
- A provider-neutral coordination-host seam must separate semantic actions and checkpoints from host mechanics.
- Current AWS FUNCTION support remains distinct from this future durable coordination host until supported packaging and operations exist.
- Other providers may implement optimised hosts if they pass the same conformance properties; infrastructure uniformity is not required.
- AWS callback/history contract questions remain suitable for direct provider validation, but they do not move TPF semantic authority.
