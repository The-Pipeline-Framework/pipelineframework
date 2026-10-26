---
title: ChatAgent Command handoffs stay inert
status: accepted
---

# ADR-0078: ChatAgent Command handoffs stay inert

## Context

ADR-0076 reserved a `ChatResult.HANDOFF` outcome but the first `chat-agent-query` Block could only reply or use one read-only Query before replying. An application that needed one owner-facing agent to recognise a business request therefore had to add a separate model router. That router could make a decision before seeing authorised conversation history and duplicated intent interpretation.

The LLM Query connector already validates a model-selected callable against a release-pinned canonical input. TPF also has native Query, Command and Await boundaries for execution. The missing capability is to return a validated Command proposal from the Block without dispatching it there.

## Decision

An LLM decision union may declare `<tpf.llm.HandoffCall>` in addition to `<tpf.llm.AgentCall>` and a completion variant. With this variant present, a selected Command callable becomes a `HandoffCall`: the connector resolves the alias from its compiled catalogue, rejects undeclared or model-supplied trusted fields, injects trusted input fields, and validates canonical arguments. Query callables continue to produce `AgentCall`. The generated dynamic invocation catalogue for that decision contains only executable Query aliases; a handoff cannot enter it.

The `chat-agent-query` Block converts `HandoffCall` to `ChatResult.HANDOFF` with carried bounded `ChatState` and a typed `ChatProposal`. It executes no Command. The consuming application validates the proposed operation, arguments, authorised scope and domain policy, then chooses its own Command or Await path. A rejected handoff fails explicitly. Native Query and provider failures remain failures. One initial inference can therefore produce a reply or handoff; a Query observation receives one synthesis inference and ends in a reply. Actions after that observation require a later, explicit composition.

The application binds finite Query and Command callable slots at build time. The Block supplies no binding, credentials, Command ID, duplicate policy or effect authority. This extends the imported callable-slot restriction recorded in ADR-0028 without changing its application-authority decision. No new runtime step kind or Agent runtime is introduced.

The first multi-alternative decision uses prompt-guided `OPTIONAL` structured output because current adapters enforce native JSON Schema only for a single direct completion. Canonical callable validation remains mandatory after inference; it does not guarantee that a model-chosen business term is acceptable. The application rejects such proposals before Command admission. The one-alternative synthesis decision retains `REQUIRED` structured output.

## Rationale

One decision process can use the same authorised conversation context for natural replies and business intent. A distinct handoff payload keeps the model's proposal separate from native effect execution and lets build-time linking exclude it from dynamic dispatch. The application remains the final authority for its business action.

## Consequences

- A Command handoff is an inert, validated proposal; its presence never means that the Command ran or that a human approved it.
- A new execution may hydrate approved history through an application-owned captured Query before ChatAgent. Storage remains optional for the Block itself.
- Applications must reject forged scope, aliases, typed arguments and invalid business terms before native Command admission; externally returned `ChatState` is not trusted by type alone.
- Query use may require two inferences. Reply and handoff use one. There is no open-ended loop, implicit post-Query action or hidden failure conversion.
