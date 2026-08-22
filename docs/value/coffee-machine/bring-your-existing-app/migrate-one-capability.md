---
title: "Can we move one capability without beginning a rewrite cult?"
faq:
  id: "migrate-one-capability"
  track: "bring-your-existing-app"
  question: "Can I migrate one capability at a time without rewriting the application?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "enterprise-edna"
      text: "A migration is only real after a steering committee approves the new target operating model."
    - persona: "test-terry"
      text: "Parallel systems are dangerous, so production should be the first and only rehearsal."
    - persona: "functional-fran"
      text: "Rewrite the whole core in one weekend; infrastructure will politely wait."
social:
  poll:
    question: "What is the first migration unit?"
    options:
      - "The repository root"
      - "The most fashionable module"
      - "Explicit typed boundary"
      - "Everything after a long"
    preferred: "Explicit typed boundary"
fortune:
  quote: "A good migration replaces responsibility at a seam, not confidence with a deadline."
related:
- "prove-the-promise"
- "untangle-without-duplicating"
- "keep-spring-boot"
tags:
- "bring-your-existing-app"
- "migrate"
- "one"
- "capability"
---

# Can we move one capability without beginning a rewrite cult?

## Elevator answer

**Yes. Start with one consequential flow, preserve its existing edges, extract typed decisions gradually, and use compatibility checks to keep behavior trustworthy throughout migration.**

<CoffeeMisconceptions />

## The real explanation

Incremental migration is not a timid version of transformation. It is the only honest response when a running application contains business knowledge that no diagram fully captures. A legacy flow embodies edge cases, exception conventions, data assumptions, and operational behaviors. Replacing all of it at once gives a team fewer places to compare meaning and more places for a regression to hide.

Choose a capability with a meaningful boundary: a flow that already suffers from repeated mapper glue, inconsistent retries, a difficult transport change, or unclear ownership after failure. Keep its existing controller, consumer, repository, or client where that is the safest edge. Extract one typed business decision at a time. Declare the connector or runtime boundary only when the original behavior has a counterpart that can be observed and tested.

The goal is not to duplicate the entire old system beside a shiny new one. It is to create a narrow seam. Existing components can provide facts to a new flow; the new flow can initially call an existing adapter. As confidence grows, a team can move one boundary at a time. Each step should answer a concrete question: what behavior now belongs to the typed core, what still belongs to the legacy shell, and what evidence shows callers see the same promise?

Compatibility is more important than ideological neatness. A migrated flow may temporarily use JPA repositories, established exception translation, or existing Kafka consumers. That is acceptable if the temporary dependency is explicit and has an owner. The migration becomes dangerous only when temporary bridges become invisible architecture and nobody can say which direction the application is moving.

TPF helps by making more of the target contract build-time visible. Step resolution, mapper compatibility, declared connectors, transport requirements, and generated artifacts can fail before deployment. That does not prove the legacy semantics are identical, but it reduces the class of accidental integration mistakes while tests and comparison runs handle the business behavior.

The trade-off is patience. A staged migration produces a period of mixed styles and demands documentation of the seam. It may feel slower than announcing a replacement. It is usually faster than discovering, late in a big-bang effort, that an old controller contained the only implementation of an obscure but important rule.

The mental model is a strangler with manners: replace responsibility at a boundary, not vocabulary across the whole codebase. The old application stays useful evidence until the new execution contract has proved it can carry the same promise.

## Trade-offs

TPF gains a reversible migration path and continuous evidence. It gives up the visual purity of a clean-slate diagram. Teams must pay attention to temporary bridges and remove or formalize them deliberately.

## When TPF is not a good fit

If no capability has a stable boundary or the team cannot invest in comparative tests, pause before migration. TPF does not turn an unowned legacy system into a safe change merely by adding YAML.
