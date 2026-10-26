# ChatAgent Block

ChatAgent packages a conversational Pipeline from an explicit `ChatState` to a proposed `ChatResult`. The basic Block makes **one LLM Query**. It needs no database, history Query, tool catalogue or Agent runtime. The application supplies the LLM binding and decides which messages and replies it accepts.

```mermaid
flowchart LR
    A[Application-authorised ChatState] --> C[ContextAssembler selects model input]
    C --> L[Captured LLM Query]
    L --> R[Proposed ChatResult]
    R --> V[Application validation and acceptance]
    V --> N[Next ChatState]
    H[Optional application history Query] --> A
    V --> P[Optional TPF persistence aspect]
```

## Start with one inference

Add `org.pipelineframework.blocks:chat-agent` as a dependency. Its `chat-agent-core` dependency supplies the portable state and pure services. Bind the Block's one Query requirement to an application-configured LLM Connector, then invoke the Block from an ordinary typed step:

```yaml
connectors:
  model:
    provider: llm.query
    version: 1
    config: { model: my-model, connection: my-model-connection }

blockBindings:
  org.pipelineframework.chat/chat-agent:
    llm.reply: { using: model }

steps:
  - name: Reply to owner
    pipeline: chat-agent
    cardinality: ONE_TO_ONE
    input: ChatState
    output: ChatResult
    java:
      input: org.pipelineframework.blocks.chat.ChatState
      output: org.pipelineframework.blocks.chat.ChatResult
```

The application constructs `ChatState.start(authorisedScope, messageId, text)` or supplies a validated state carried from a previous step. `ConversationScope` must come from an authenticated host or trusted application state. If a client returns `ChatState`, validate its owner, tenant, conversation, message identity, integrity and eligible evidence before using it. The type alone is not proof of authority.

`ChatResult` has exactly one disposition. `REPLY` contains a proposed natural reply and proposed next state; the application validates and accepts both before making them durable or using `next(...)` for another message. The Query variant can also produce `HANDOFF`: a validated, inert `ChatProposal` carrying a pinned application Command identity and canonical arguments. The application checks scope and policy before admitting its own Command. Neither Block executes a Command or grants business authority. Native Query or provider failures fail the execution.

`ChatState` permits at most 64 evidence entries, 16 KiB of UTF-8 text per entry and 256 KiB of carried state. Appending evidence or a proposed reply evicts oldest entries deterministically and increments `omittedEntries`. A new state with an oversized current message or invalid shape is rejected. Large immutable content should stay in an owned `PayloadReference` with a bounded excerpt; a reference is not automatic permission to disclose its bytes to the model. These limits apply independently of `ChatWindow` and `ContextAssembler`, which select the smaller model-visible context. If the current message cannot fit, inference fails before calling the provider.

## Add a typed Query or business handoff

Choose `org.pipelineframework.blocks:chat-agent-query` when the model may need one application-approved read before answering. This variant uses the same `ChatState` and `ChatResult` contract. Bind two LLM Query requirements and a finite catalogue of typed, one-to-one Query operations:

```yaml
blockBindings:
  org.pipelineframework.chat/chat-agent-query:
    llm.decide: { using: model }
    llm.synthesize: { using: model }
    tools:
      court_guide:
        using: application
        operation: court.guide
        operationVersion: 1
        input: CourtGuideRequest
        config: { label: public-court-guide }
      owner_action:
        using: application
        operation: owner.action.apply
        operationVersion: 1
        kind: command
        input: OwnerAction
        trustedArguments:
          requestId: state.messageId
          ownerId: state.scope.ownerId
          ownerText: state.message
        config: { label: proposed-owner-action }

steps:
  - name: Reply with optional court guide
    pipeline: chat-agent-query
    cardinality: ONE_TO_ONE
    input: ChatState
    output: ChatResult
    java:
      input: org.pipelineframework.blocks.chat.ChatState
      output: org.pipelineframework.blocks.chat.ChatResult
```

The first LLM Query completes directly, proposes one Query, or proposes a Command handoff. Direct completion and handoff use one inference. TPF validates the Query arguments and dispatches exactly one native Query; its captured observation becomes bounded carried evidence and a second LLM Query writes a natural answer. A Command handoff ends the Block immediately. It contains canonically validated arguments and trusted fields copied from the authorised input, but no Command has run. The application must check the `ChatResult` disposition, operation identity, scope, integrity and business terms before its own typed Command step. A failed provider or Query call remains a native failure; an oversized observation must be shortened by the application Query or represented by a suitable `PayloadReference`.

The first decision uses `structuredOutputSchema: OPTIONAL`: current adapters cannot enforce a native response schema while the model chooses among completion and multiple callables. TPF still rejects unknown aliases, malformed arguments and canonical type violations after the inference; it does not repair or retry a bad proposal. The synthesis Query has one direct completion and retains the default `REQUIRED` mode. Describe application-specific action codes and required terms in the authorised context, and validate them again in the consuming application.

The application owns each callable's provider binding, operation version, input type, host authorisation and disclosure. The compiler rejects missing bindings, unsupported operation versions, type mismatches and non-one-to-one Query tools. It keeps Command handoff aliases out of dynamic Query dispatch. Tool availability is a release-pinned allowlist, not runtime discovery. Both variants share the `chat-agent-core` library and its pure `chat-context` preparation Block, so an application may install either variant or both without duplicating canonical types.

## Add durable continuity when needed

For a new execution, the application may read previously **accepted** turns through a scoped Query and construct `ChatState` from that captured observation. It may persist accepted owner and agent turns through TPF's persistence aspect. The Block does not choose a table, provider, retention policy or erase behaviour. A computation already carrying authorised state does not need a history read. PadelPot demonstrates both optional hydration at its new chat root and native persistence of accepted turns, while its existing negotiation and Await path remains application-owned.

The model sees selected text as evidence. It may miss a correction, misstate certainty or follow malicious instructions embedded in history or tool results. Preserve typed policy checks and human approval for commitments. More powerful tool, MCP, recursive research, browser and code compositions can build on the same typed Query/Command/Await boundaries; they are not part of this bounded Block.

See [Conversation context](/develop/connectors/conversation-context), [LLM Query](/develop/extension/llm-query), [ADR-0076](/decisions/0076-chat-agent-carries-bounded-conversation-state) and [ADR-0078](/decisions/0078-chat-agent-command-handoffs-stay-inert) for the underlying contracts and ownership.
