---
title: "Does reactive mean nobody may block ever again?"
faq:
  id: "reactive-not-a-personality-test"
  track: "runtime"
  question: "Does TPF force reactive programming on every developer?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "async-andy"
      text: "Blocking is a moral failure, not a scheduling fact."
    - persona: "jpa-jane"
      text: "Every database call belongs on the event loop."
    - persona: "async-andy"
      text: "More threads are a substitute for backpressure."
social:
  poll:
    question: "Blocking work goes…"
    options:
      - "On the event loop"
      - "In every callback"
      - "Off the event loop"
      - "Into a TODO"
    preferred: "Off the event loop"
fortune:
  quote: "Reactive is not a personality test; it is a promise not to block everyone else."
related:
- "backpressure-is-a-promise"
- "retry-is-not-for-rejection"
tags:
- "runtime"
- "reactive"
- "personality"
- "test"
search: false
---

# Does reactive mean nobody may block ever again?

## Elevator answer

**You may write ordinary Java. Just do not make 300 flows wait behind one blocking JDBC or PDF call on the event-loop thread; offload it deliberately.**

<CoffeeMisconceptions />

## The real explanation

Reactive is not a personality test. It is a runtime response to many flows waiting on HTTP, Kafka, or durable completion at once. The danger is concrete: one step renders a PDF or performs blocking JDBC work on an event-loop thread, and hundreds of unrelated requests queue behind it while every individual method looks innocent.

Business code can still be ordinary Java. A typed decision may be simple and synchronous. The important rule is that genuine blocking work—JPA calls, legacy clients, CPU-heavy transformations, file operations—must be identified and explicitly offloaded in the runtime path that uses it. That makes the cost visible and prevents a convenient dependency from silently consuming the executor that carries many other requests.

Reactive execution does not abolish transactions, exceptions, or debugging. It changes where teams must be precise about context propagation, scheduling, timeout, and concurrency. TPF’s role is to keep those runtime semantics owned by the shell rather than forcing every business step to invent them.

The trade-off is learning. Teams must know which work blocks and validate it in the deployed runtime. That is less pleasant than declaring all code non-blocking and discovering otherwise under load.

## Trade-offs

TPF gains safer high-concurrency execution. It gives up hidden blocking convenience. Teams must offload and test blocking paths explicitly.

## When TPF is not a good fit

If an application is small, entirely synchronous, and has no consequential concurrency demand, reactive runtime machinery may not earn its complexity.
