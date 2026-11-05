# Authored-consumer migration evidence

Compiler issue [36](https://github.com/The-Pipeline-Framework/pipelineframework-compiler/issues/36)
enforces [one authored consumer per concrete canonical input](/decisions/0081-one-authored-consumer-per-concrete-input)
after proving an executable PadelPot migration. Local fixture evidence is separate from coordinated
candidate system tests: the exact participating PR heads must pass the shared compatibility gate
before merge.

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

The application fixture suite passed before authored-consumer rejection was enabled. Candidate
system tests must also pass for the coordinated PR heads before merge; local cache warming is
not candidate compatibility evidence. See [cross-repository system tests](./cross-repository-system-tests).

[ADR-0086](/decisions/0086-explicit-self-invocation-and-nested-await) records explicit self-invocation and nested suspension. LOCAL unary child completion returns through every caller. The selected-outcome and persistence rules are described by [ADR-0085](/decisions/0085-select-canonical-outcomes-for-await-and-observation).
