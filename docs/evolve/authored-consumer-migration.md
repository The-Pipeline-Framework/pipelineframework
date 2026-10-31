# Authored-consumer enforcement prerequisites

Compiler issue [36](https://github.com/The-Pipeline-Framework/pipelineframework-compiler/issues/36)
requires [one authored consumer per concrete input](/decisions/0081-one-authored-consumer-per-concrete-input).
Enforcement must follow a working PadelPot migration. The existing authoring rule alone
does not prove that migration is expressible.

## Source evidence

PadelPot's root definition repeats `TurnState`, `AgentResult`, `Review`, and
`AnsweredReview` consumers. `RequestReview` and `RequestFinalReview` are identity
`Review → Review` services used to attach Await. The personal-agent definition also
consumes `TurnState` in its initial Query and its recursive invocation. Moving root
steps into nested definitions alone therefore does not resolve the complete migration.

The migration constraints have distinct owners:

| Constraint | Source authority | Required proof |
| --- | --- | --- |
| Identity-free branch completion needs a checked definition output boundary. | Compiler `PipelineBranchRoutingPlanner` and `PipelineBranchingMetadataGenerator`; runtime `PipelineBranchingRegistry` and `PipelineRunner`. | Implemented: independent branch results complete through the explicit output contract, with checked concrete extraction and union construction. |
| Recurrence must preserve one authored consumer. | Contracts `PipelineTemplateDefinition`; compiler `PipelineRecurrenceLowering`, `PipelineDefinitionLinker`, `LocalPipelineInvocationRenderer`; runtime `PipelineInvocationSteps`. | Implemented for local unary definitions: `recur` lowers to the existing bounded invocation path, retaining deterministic frames and context. |
| Nested definitions reject Await, and a pipeline invocation cannot carry Await. | Contracts `PipelineTemplateConfigLoader.readV3PipelineDefinitions`; compiler `StepDefinitionParser`. | Deferred completion resumes at the correct operation and definition boundary, without re-running effects or losing the continuation. |
| An internal operation's Await applies to every emitted result. | Compiler `DeferredCompletionStepRenderer`; runtime `AwaitCompletionSupport`. | Only the interaction-requiring branch suspends when a meaningful operation produces both review and terminal outcomes; other results preserve their contracts. |
| Shared observer names can collide across definition scopes. | Compiler `AspectExpansionProcessor`. | Both definition execution orders contain the observer even when its generated implementation name is shared. |

Observer preservation, definition completion, and local unary definition recurrence now have implementation and regression coverage.
[ADR-0082](/decisions/0082-definition-output-completes-typed-branches) describes completion against
the existing explicit output contract, including runtime union adaptation.
[ADR-0083](/decisions/0083-definition-owned-bounded-recurrence) supplies the `recur` declaration.
The deferred-completion migration remains a prerequisite; nested Await and root recurrence are
not supplied by these changes.

## Implementation order

1. Implement definition completion and structural bounded recurrence in the shared
   model, DSL, compiler, generated contracts, and runtime. Preserve declared union
   identity at return boundaries rather than returning an unchecked concrete payload.
2. Implement the deferred-completion boundary needed by a meaningful PadelPot operation.
   Prove suspension, correlated completion, duplicate admission, timeout, and exact
   continuation, including any nested context. Do not replace identity services with
   identity Commands or Query providers.
3. Migrate the complete PadelPot definition and its executable fixture. Preserve its
   negotiation policy and human-authorisation boundaries. Do not silently replace
   bounded recurrence with an arbitrary fixed number of unrolled turns.
4. Enable authored-consumer validation before model/aspect expansion. Cover concrete
   and union collisions, disjoint variants, aliases, non-adjacent consumers, reproduced
   types, Query/Command/Await pairs, independent definitions, and generated observers.
5. Run the owner suites and retain a passing `tpf/system-tests` result for the exact
   PR head before merge. When component PRs participate, require a coordinated
   compatibility set covering their current heads, with success recorded on every
   participating SHA; otherwise retain this PR's singleton result. Update
   telemetry/replay rendering and authoring examples with the resulting lifecycle
   contract.

The strict invariant includes authored recursive calls. Structural recurrence is a
definition lifecycle boundary, not a validation exception or a second application
execution path. Local unary recurrence now lowers through the existing bounded
invocation path. The Await migration and complete PadelPot fixture still require
implementation and executable evidence before rejection can be enabled.
