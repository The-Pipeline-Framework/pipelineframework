---
title: ChatAgent carries bounded conversation state
status: accepted
---

# ADR-0076: ChatAgent carries bounded conversation state

## Context

ADR-0075 placed accepted conversation history and durable storage under application authority. Its example reads history through a Query before every model turn. That is useful when starting a new execution from stored dialogue, but it is unnecessary when a computation already carries the authorised context. Requiring that Query in a reusable ChatAgent Block would make persistence a prerequisite for a simple conversation and would couple the Block to one application's history policy.

ADR-0033 shows that a Block can package agentic composition without acquiring connector or Command authority. A conversational Block needs an equally clear contract for state, context limits, one-turn inference and optional tools.

## Decision

`ChatState` is an explicit typed input and proposed output. It carries the current message, authorised conversation scope, bounded prior evidence and model-window settings. The application admits and validates this state; an externally returned state is never trusted merely because it has the right type. The application owns scope, integrity, disclosure, turn acceptance, and any persistence or hydration. A new execution may obtain history through an application-owned, captured Query, then construct `ChatState`. The storage-free path starts directly from an application-authorised `ChatState`.

The `chat-agent` Block selects model-visible evidence with `ContextAssembler`, makes one ordinary LLM Query, and returns a proposed natural reply with next state. `ChatState` has its own fixed entry-count and byte bounds, with deterministic oldest-first eviction; model-context selection is an independent, smaller budget. Large immutable content is carried by `PayloadReference` with a bounded excerpt when appropriate. A required current message that cannot fit the model budget fails before inference.

The optional `chat-agent-query` Block accepts an application-bound catalogue of typed, one-to-one Query tools. One LLM Query either completes directly or proposes one allowed Query. Native dynamic Query dispatch captures the observation. A pure reducer adds bounded evidence to carried state and a second LLM Query produces a natural reply. The Block has no recursive loop. A provider or native Query failure remains a failure. Neither Block executes an application Command or converts a failure into a successful handoff. The public result reserves an inert handoff for later compositions; any producer must validate the proposal against a pinned contract before emission, and the consuming application handles it through its own Query, Command or Await steps.

The compiler links tool aliases and provider type contracts at build time. The application chooses bindings, operation versions and tool eligibility. The Block uses existing Query capture, replay, branching and typed composition; there is no Agent runtime, hidden conversation store, mandatory history Query or new execution primitive.

Implementation exposed two narrow contract gaps: generated domain/protobuf contracts must import a packaged Block's declared types, and the runtime must use the compiler-pinned Java class for a canonical union payload that lives in another package. The compiler and canonical type catalogue supply those links; package-name guessing cannot represent the declared type contract.

## Rationale

Carrying what the computation already knows avoids repeated storage reads and preserves explicit data coupling between steps. Keeping hydration optional lets a short-lived proof start with one dependency and one model binding, while an application that needs durable continuity can add persistence and a captured boundary Query under its own policy. Separate carried-state and model-context bounds prevent a small prompt from masking unbounded pipeline payload growth.

## Consequences

- Applications validate external admission and proposed next state before accepting a turn. A model reply is not an owner approval or a business commitment.
- The basic Block requires only an LLM Query binding. Typed Query tools add a finite catalogue, a second LLM binding and one optional second inference; Query failures retain native failure semantics.
- Applications decide whether and when to persist accepted turns. TPF's persistence aspect remains the storage mechanism when selected. No Connector-owned transcript tables are introduced.
- Tool calling, recursive research, MCP, browser and code capabilities can be added as typed, application-authorised compositions later. This decision does not grant them implicitly.
- ADR-0075 remains authoritative for ownership of approved durable history. Its per-execution history-read example is optional for ChatAgent, as clarified here.
