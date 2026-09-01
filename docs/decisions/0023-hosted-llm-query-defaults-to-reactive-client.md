---
title: Hosted LLM Query defaults to reactive provider I/O
status: accepted
---

# ADR-0023: Hosted LLM Query defaults to reactive provider I/O

## Context

The OpenAI-compatible LLM Query connector exposes one provider identity and one asynchronous
decision contract while permitting the host to supply either a LangChain4j `StreamingChatModel` or
`ChatModel`. Both implementations are valid, but an omitted runtime selector must choose one. LLM
inference is long-running network I/O, and choosing the blocking implementation by default consumes
an offload thread for the duration of every live request even when the host supports non-blocking
provider callbacks.

## Decision

`llm.query.openai.compatible` defaults its runtime client implementation to `reactive`. The reactive
implementation resolves an `AuthenticatedOpenAiCompatibleReactiveConnection` and consumes the
provider callback without occupying an offload thread. Pipeline YAML, connector provider identity,
Query capture, structured-output policy, and the application completion contract remain unchanged.

Blocking hosts remain supported through the same connector. They must set
`pipeline.llm.langchain4j.openai-compatible.client-implementation=blocking` and resolve an
`AuthenticatedOpenAiCompatibleConnection` backed by a LangChain4j `ChatModel`. Blocking provider
calls continue to use framework-owned offload rather than running on an event-loop thread.

## Rationale

Reactive execution is the safer framework default for latency-bound provider I/O and best aligns
with TPF's reactive execution model. Keeping selection in host runtime configuration preserves one
portable pipeline contract while allowing blocking Spring or Quarkus deployments to opt into the
implementation matching their stack.

## Consequences

- A host that previously relied on the omitted selector and supplies only a `ChatModel` connection
  must opt into `blocking` explicitly.
- Reactive hosts supply a `StreamingChatModel` connection and need no selector property.
- This decision does not convert the Ollama adapter or unrelated blocking connectors; it governs the
  dual-implementation OpenAI-compatible LLM Query connector.
- The implementation choice remains deployment configuration, not pipeline DSL or connector
  identity.
