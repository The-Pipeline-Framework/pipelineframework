---
search: false
---

# External Library Delegation

External delegation lets a pipeline call existing Java behavior without hiding the boundary from TPF.

Use it when the business flow should stay typed and explicit, but one step is already implemented by another method, library, or operator surface.

## Choose The Right Path

| Need | Use |
| --- | --- |
| Call an existing local Java method with compatible input/output types | [Use Existing Java Methods](/versions/v26.6.2/develop/extension/use-existing-java-methods) |
| Call a reusable operator service with its own DTO/entity model | [Operator Service Contracts](/versions/v26.6.2/develop/extension/operator-service-contracts) |
| Move old annotation-driven delegation into YAML-first configuration | [Legacy Delegation Migration](/versions/v26.6.2/develop/extension/legacy-delegation-migration) |

## Design Rule

Keep the pipeline contract in YAML and Java types. Let TPF validate the boundary:

- the step name and operator reference,
- input and output type compatibility,
- mapper selection,
- cardinality compatibility,
- transport compatibility.

If the delegated code performs I/O, retries, or correlation internally, make that behavior visible in the pipeline design. Prefer [Await Boundaries](/versions/v26.6.2/design/await-boundaries), [Object Ingest](/versions/v26.6.2/design/object-ingest), or [Checkpoint Handoff](/versions/v26.6.2/deploy/orchestrator-runtime/checkpoint-handoff) when TPF owns the shell better than application code.

## Reference

The longer implementation reference is preserved at [Operator Delegation Reference](/versions/v26.6.2/develop/extension/operator-delegation-reference).
