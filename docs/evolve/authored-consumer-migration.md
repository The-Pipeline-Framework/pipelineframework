# Authored-consumer migration evidence

Compiler issue [36](https://github.com/The-Pipeline-Framework/pipelineframework-compiler/issues/36)
enforces [one authored consumer per concrete canonical input](/decisions/0081-one-authored-consumer-per-concrete-input)
after proving an executable PadelPot migration. The coordinated component PRs and the PadelPot
application migration merged on 5 November 2026.

## Migration

The earlier root repeated `TurnState`, `AgentResult`, `Review`, and `AnsweredReview` consumers.
Its identity review services carried Await, and its explicit recursive call repeated the initial
Query's input. The migrated root calls the named personal-agent definition. Its explicit self-call accepts TurnState, and the root completes Result. It removes the identity services and `AgentEnd`, `AgentResult`, and `LoopEnds` wrappers.

The meaningful policy Commands can produce a continuing state, a Review, or a Result. Their
Await modifiers select Review and complete it as AnsweredReview. The final TurnCompletion union
describes the actual resulting leaf set. Persistence aspects select Review and Result at their
producing operations, preserving the pipeline carrier. One human response operation owns review
policy through trusted `reviewRound` state. Model output cannot replace that state.

[PadelPot PR #6](https://github.com/The-Pipeline-Framework/padelpot/pull/6) retains the current
reusable ChatAgent integration. Its fixture suite exercises the application with aspect-owned persistence. The compiler's dependency-free YAML fixture preserves the
original issue baseline; the application suite covers the newer Block integration.

## Owner evidence

| Contract | Regression evidence |
| --- | --- |
| Concrete aliases and nested union payloads expand consistently. | Contracts `ConcreteTypeNamesTest`; compiler `AuthoredConsumerValidatorTest`. |
| Authored consumers are checked before generated lifecycle steps. | `PipelineDiscoveryPhaseTest` rejects separated/reintroduced consumers; discovery resolves self-calls in both template and parsed definitions. |
| Independent definitions retain observers with shared implementations. | Existing `AspectExpansionProcessorTest` and definition-order tests cover both expansion orders and incompatible domains. |
| Root recurrence uses existing linking and rendering. | `PipelineRootRecurrenceLoweringTest` checks the generated self-invocation and replay continuation. |
| Selected Review Await preserves continuing and terminal outcomes. | `AwaitBranchSelectionResolverTest`, `DeferredCompletionStepRendererTest`, and metadata tests cover selected payloads, transformed leaf sets, observer occurrences, and carrier contracts. |
| Durable suspension preserves the recurrence bound. | `AwaitRootRecurrenceStateTest`, `RootRecurrenceInteractionStoreTest`, and `PipelineRootRecurrenceAwaitTest` cover fresh-coordinator loading, store/worker-codec round trips, exact resume position, and depth after resume. |
| Repeated qualified waits have separate authority. | `AwaitRecurrenceOccurrenceTest`, `AwaitCycleAdvanceTest`, and execution-state store tests cover bounded stable invocation identities, duplicate/late release, and scalar/itemized Dynamo acknowledgement scope. |
| The complete application remains executable. | PadelPot's fixture suite exercises correlated human resume, two reviews in one root, concurrency, private history, aspect-owned persistence, and terminal branches. The compiler retains its migrated YAML under `compatibility/padelpot`. |

The [coordinated compatibility run](https://github.com/The-Pipeline-Framework/pipelineframework/actions/runs/41940174786)
passed for the exact contracts, compiler, runtime, Blocks and examples heads that merged.
PadelPot was outside that set. Its 63 fixture tests passed separately, and its default and
Ollama configurations packaged successfully before merge. These application checks used
the worktree's local dependency cache; they do not extend the coordinated gate to PadelPot.
See [cross-repository system tests](./cross-repository-system-tests) for the gate's scope.

[ADR-0086](/decisions/0086-explicit-self-invocation-and-nested-await) records explicit self-invocation and nested suspension. LOCAL unary child completion returns through every caller. The selected-outcome and persistence rules are described by [ADR-0085](/decisions/0085-select-canonical-outcomes-for-await-and-observation).
