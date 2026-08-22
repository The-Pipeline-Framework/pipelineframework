---
title: "Is Java now part of the architecture contract?"
faq:
  id: "java-and-other-languages"
  track: "governance"
  question: "Does TPF lock us into Java because generated types are central?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "microservice-mike"
      text: "One language is one architecture."
    - persona: "consultant-nigel"
      text: "Every service needs a different runtime."
    - persona: "consultant-nigel"
      text: "Portable means identical everywhere."
social:
  poll:
    question: "TPF is…"
    options:
      - "Language neutral"
      - "Java-first"
      - "Java-first"
      - "A universal compiler"
    preferred: "Java-first"
fortune:
  quote: "A clear Java-first boundary is more useful than a vague promise of language neutrality."
related:
- "typing-and-evolution"
- "portability-without-handwaving"
tags:
- "governance"
- "java"
- "other"
- "languages"
---

# Is Java now part of the architecture contract?

## Elevator answer

**TPF is Java-first today, while external services may join through explicit contracts; portability should preserve business semantics without pretending every language has identical support.**

<CoffeeMisconceptions />

## The real explanation

TPF’s core and generated application surfaces are Java-first. That is a real constraint, not a detail to hide. Strong typing and generation rely on types and compiler support in the primary ecosystem. Kotlin or other JVM languages may participate where the relevant paths support them, but teams should not claim parity without validation.

Non-JVM services can still join a business journey through stable transport and connector contracts. They do not need to import the pipeline runtime to receive an API, command, or event. The boundary must remain explicit so that language portability does not become semantic vagueness.

The trade-off is honest scope. Java-first tooling may be exactly right for a Java organization; it is not a reason to pretend the framework is the universal internal language.

## Trade-offs

TPF gains deep typed integration in its primary ecosystem. It gives up universal language parity.

## When TPF is not a good fit

If the primary application estate is not Java-oriented and needs first-class uniform tooling across languages, choose a more suitable center.
