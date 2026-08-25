---
title: "Do I have to replace Spring Boot to get any value?"
faq:
  id: "keep-spring-boot"
  track: "bring-your-existing-app"
  question: "Does adopting TPF require replacing Spring Boot?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "spring-sam"
      text: "If a new framework arrives, immediately delete every starter and begin a fasting migration."
    - persona: "platform-priya"
      text: "Two runtime concerns cannot coexist; one must win a ceremonial dependency-resolution duel."
    - persona: "consultant-nigel"
      text: "Call it transformation, schedule eighteen workshops, and prohibit the word incremental."
social:
  poll:
    question: "What must be replaced first?"
    options:
      - "Every Spring starter"
      - "The entire service layer"
      - "Typed business flow"
      - "The team’s coffee machine"
    preferred: "Typed business flow"
fortune:
  quote: "A migration is credible when it preserves what already works and makes one costly uncertainty smaller."
related:
- "migrate-one-capability"
- "controllers-are-not-the-enemy"
- "spring-in-a-pipeline-hat"
tags:
- "bring-your-existing-app"
- "keep"
- "spring"
- "boot"
search: false
---

# Do I have to replace Spring Boot to get any value?

## Elevator answer

**No. Keep Spring Boot hosting the application. Pick the one service method that loads three rows, calls two APIs, retries one, and publishes a message; that is the candidate, not the whole bean factory.**

<CoffeeMisconceptions />

## The real explanation

Spring Boot can keep creating beans, loading configuration, exposing health endpoints, and hosting the controller your runbook already knows. TPF is not asking the application to renounce starters and live in a YAML monastery.

Look for a concrete sore spot: `PaymentService.process()` loads an invoice and customer, asks a fraud API, charges a card, saves three status changes, then publishes Kafka—with retry annotations spread across two classes. Keep the Spring controller and adapters. Extract the decisions and their typed dataflow; make the fraud lookup a Query and the charge an external Command. Now the order and data dependencies are reviewable without replacing every surrounding bean.

The first slice should preserve the working edge and compare behavior. If the old controller maps `CardDeclinedException` to 422, keep that promise until the team deliberately changes it. Framework compilation can catch incompatible mappings, missing connectors, and unsupported generated paths; characterization tests still have to prove the pricing rule and failure behavior.

Before choosing the slice, verify the exact host path you intend to use: compiler support, generated adapters, transport mode, lifecycle integration, and a representative smoke example. “TPF supports runtimes” is not evidence that your particular Spring deployment shape works. If one capability is missing, leave it in Spring behind an honest adapter.

Coexistence is fine when it has a direction. Each new TPF capability should retire some old plumbing: a retry loop, mapper chain, orchestration service, or hand-written adapter. If migration only adds files and deletes nothing, it is not migrating. It is collecting architectures.

## Trade-offs

TPF gains an incremental path while Spring keeps doing useful Spring work. The cost is a temporary mixed style, a documented seam, and evidence that the chosen runtime path actually works.

## When TPF is not a good fit

If the application is entirely local and Spring code already exposes every relevant contract clearly, migration may not earn its cost. Do not replace a stable framework merely to make the technology list more interesting.
