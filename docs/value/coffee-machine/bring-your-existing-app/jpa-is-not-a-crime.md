---
title: "Will JPA become a forbidden relic?"
faq:
  id: "jpa-is-not-a-crime"
  track: "bring-your-existing-app"
  question: "What happens to our existing JPA repositories and lazy-loading behavior?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "jpa-jane"
      text: "A lazy proxy is a domain concept because it appears at runtime."
    - persona: "functional-fran"
      text: "Ban persistence immediately; the database will understand the gesture."
    - persona: "enterprise-edna"
      text: "Add one base repository and every aggregate boundary will heal."
social:
  poll:
    question: "What should a business step receive?"
    options:
      - "An open session and optimism"
      - "Every table in the schema"
      - "Typed flow contract"
      - "A proxy with a"
    preferred: "Typed flow contract"
fortune:
  quote: "Lazy loading is not evil; it is simply a very expensive way to keep a boundary implicit."
related:
- "untangle-without-duplicating"
- "prove-the-promise"
tags:
- "bring-your-existing-app"
- "jpa"
- "crime"
---

# Will JPA become a forbidden relic?

## Elevator answer

**JPA may remain an adapter, but pipelines should receive explicit typed facts rather than allowing persistence sessions and lazy loading to define business execution.**

<CoffeeMisconceptions />

## The real explanation

JPA is often where a legacy application hides its real coupling. Repositories carry useful persistence behavior, while lazy loading lets ordinary-looking domain code reach back into an open session for more data. That can be convenient, and it can also make business behavior depend on transaction scope, query shape, and the accidental availability of infrastructure.

TPF does not require throwing repositories away. It asks a more precise question: what facts does a typed business decision actually need, and where should they be obtained? A persistence adapter can load the required data, map it to an explicit input, and let the core make a decision without discovering new database work halfway through a transformation. The adapter remains free to use JPA; the business flow no longer needs a live session as an undocumented collaborator.

Migration should be careful. Replacing lazy access with eager loading everywhere can create performance regressions, and copying entities into parallel DTO hierarchies can duplicate the model. Start with one flow, observe the actual reads, define the smallest typed facts needed by the decision, and make loading behavior explicit. A repository may stay behind the adapter for a long time. The immediate win is not a new ORM; it is making data admission visible.

This matters for retries and replay. A business step whose behavior depends on whatever a lazy proxy fetches later is difficult to reproduce. A flow that records or declares its input facts has clearer execution semantics. It does not make data timeless, but it tells operators and tests what the decision was based on.

The trade-off is that explicit loading reveals queries that were previously convenient but hidden. Teams may need to design read models, batch access, or separate command/query shapes. That is real work. It is also often the work that prevents a persistence implementation detail from becoming the application’s de facto architecture.

## Trade-offs

TPF gains explicit persistence boundaries and more reproducible decisions. It gives up invisible lazy convenience. Teams must manage query performance deliberately rather than relying on a session that happens to remain open.

## When TPF is not a good fit

If a local transaction is simple and lazy loading is well-controlled, a migration may not earn its cost. Do not treat JPA as a moral failure; treat unowned persistence behavior as a design question.
