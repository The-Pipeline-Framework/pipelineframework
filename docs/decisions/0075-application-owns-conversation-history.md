---
title: Application owns approved conversation history
status: accepted
---

# ADR-0075: Application owns approved conversation history

## Context

Applications that use one-turn LLM Queries may need earlier exchanges for a later turn. A generic Connector-owned transcript store would duplicate durable business history that an application can already declare through TPF persistence. Making a Connector detect whether persistence exists and choose a fallback would also move retention and disclosure policy away from the application.

## Decision

The application defines which conversation turns are accepted business records, their identity and scope, and their retention and erasure policy. It may persist a typed value at a selected boundary through TPF's persistence aspect. A scoped application Query reads the stored history; TPF captures that observation for replay. The application explicitly selects model-visible evidence from the Query result before a one-turn LLM Query. An authorised erase or external memory write is an explicit Command.

The Connectors repository may contribute portable conversation/context vocabulary, deterministic assembly and provider adapters for genuinely external memory services. It does not own a generic transcript database, application schema, hidden persistence detection, or a fallback store. This decision does not create a universal Agent state or loop.

## Rationale

Conversation turns can include private owner text and approved agent output. Their authority, consent and lifetime are application decisions. Reusing the persistence aspect keeps storage mechanics in TPF while avoiding a second transcript authority in a Connector. Query capture and Command effects retain their separate replay and idempotency roles.

## Consequences

- Applications declare a concrete persisted value and selected step boundary, then test restart, duplicate admission, ordering, scope and erasure against the chosen store.
- Build-time validation can check type mappings, operation bindings and aspect targets. Runtime provider availability and host authorisation still need deployment and application evidence.
- A blocking persistence provider must be offloaded; an available reactive provider may be selected without changing the conversation contract.
- If application proofs reveal that whole-step persistence cannot express a suitable approved-turn projection, improve the persistence plugin at its semantic owner rather than adding Connector storage detection.
