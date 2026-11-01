# Authored-consumer migration evidence

Compiler issue [36](https://github.com/The-Pipeline-Framework/pipelineframework-compiler/issues/36)
enforces [one authored consumer per concrete canonical input](/decisions/0081-one-authored-consumer-per-concrete-input)
after proving an executable PadelPot migration.

## Migration

The earlier root repeated `TurnState`, `AgentResult`, `Review`, and `AnsweredReview` consumers.
Its identity review services carried Await, and its explicit recursive call repeated the initial
Query's input. The migrated root uses definition-owned `recur: [TurnState]` and checked `Result`
completion. It removes the identity services and `AgentEnd`, `AgentResult`, and `LoopEnds` wrappers.

Two native Commands publish approved review and final-result history through the configured TPF
persistence shell. Review publication has ordinary post-operation Await, so terminal results pass
through without an identity review operation or a branch-selective Await extension. One human
response operation owns first/final review policy through the trusted `reviewRound` state field.
Model output cannot replace that trusted state. The agent still uses one Query per observation and
the existing bounded recursive invocation path.

[PadelPot PR #6](https://github.com/The-Pipeline-Framework/padelpot/pull/6) applies the migration to
current main while retaining its reusable ChatAgent integration. Its 61 fixture tests and default
and Ollama packages pass. The compiler's dependency-free YAML fixture preserves the original issue
baseline; the application suite covers the newer Block integration.

## Owner evidence

| Contract | Regression evidence |
| --- | --- |
| Concrete aliases and nested union payloads expand consistently. | Contracts `ConcreteTypeNamesTest`; compiler `AuthoredConsumerValidatorTest`. |
| Authored consumers are checked before generated lifecycle steps. | `PipelineDiscoveryPhaseTest` rejects separated/reintroduced consumers; recurrence discovery retains `$recur` after validation. |
| Independent definitions retain observers with shared implementations. | Existing `AspectExpansionProcessorTest` and definition-order tests cover both expansion orders and incompatible domains. |
| Root recurrence uses existing linking and rendering. | `PipelineRootRecurrenceLoweringTest` checks the generated self-invocation and replay continuation. |
| Native publication precedes transport Await. | `CommandCallbackParserTest` and `PostOperationCommandAwaitRenderingTest` preserve immediate/final contracts and operation/aspect/modifier order. |
| Durable suspension preserves the recurrence bound. | `AwaitRootRecurrenceStateTest`, `RootRecurrenceInteractionStoreTest`, and `PipelineRootRecurrenceAwaitTest` cover fresh-coordinator loading, store/worker-codec round trips, exact resume position, and depth after resume. |
| Repeated qualified waits have separate authority. | `AwaitRecurrenceOccurrenceTest`, `AwaitCycleAdvanceTest`, and execution-state store tests cover bounded stable invocation identities, duplicate/late release, and scalar/itemized Dynamo acknowledgement scope. |
| The complete application remains executable. | PadelPot's fixture suite exercises correlated human resume, two reviews in one root, concurrency, private history, durable publication, and terminal branches. The compiler retains its migrated YAML under `compatibility/padelpot`. |

The application fixture suite passed before authored-consumer rejection was enabled. Candidate
system tests must also pass for the coordinated PR heads before merge; local cache warming is
not candidate compatibility evidence. See [cross-repository system tests](./cross-repository-system-tests).

[ADR-0082](/decisions/0082-definition-output-completes-typed-branches),
[ADR-0083](/decisions/0083-definition-owned-bounded-recurrence), and
[ADR-0084](/decisions/0084-root-recurrence-preserves-durable-await) record the semantic ownership.
Await inside nested definitions, invocation-level Await, mutual recursion, and streaming recurrence
remain unsupported.
