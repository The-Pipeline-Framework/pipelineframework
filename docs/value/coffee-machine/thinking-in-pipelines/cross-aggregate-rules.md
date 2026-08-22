---
title: "Where do rules go when two aggregates start arguing?"
faq:
  id: "cross-aggregate-rules"
  track: "domain-modelling"
  question: "Where do cross-aggregate rules live?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "enterprise-edna"
      text: "Put both aggregates in one transaction and call the resulting object EnterpriseAggregate."
    - persona: "kafka-frank"
      text: "Publish an event, wait an unspecified amount of time, and consistency will become a cultural value."
    - persona: "ddd-diego"
      text: "If two aggregates interact, one of them must be renamed Context and promoted immediately."
social:
  poll:
    question: "What solves a cross-aggregate rule?"
    options:
      - "One aggregate to rule them all"
      - "A Kafka topic and"
      - "Explicit business policy"
      - "An annotation with ambition"
    preferred: "Explicit business policy"
fortune:
  quote: "A pipeline can coordinate a consistency conversation; it cannot make independent ownership disappear."
related:
- "rich-domain-not-a-hostage"
- "sagas-without-slogan"
- "bounded-contexts-without-framework-gossip"
tags:
- "domain-modelling"
- "cross"
- "aggregate"
- "rules"
---

# Where do rules go when two aggregates start arguing?

## Elevator answer

**Keep each aggregate’s invariants local; express cross-aggregate policy as typed domain behavior, then let a pipeline coordinate facts and durable external consequences.**

<CoffeeMisconceptions />

## The real explanation

Cross-aggregate rules are where architecture slogans meet an actual business. An aggregate is useful precisely because it limits the state that must change consistently. But businesses regularly ask questions that span those limits: can a customer consume more credit while a reservation is pending; may an order be fulfilled while a compliance case is open; does a promotion apply across catalog and customer facts? Pretending these are all one aggregate merely moves the difficulty into a larger transaction boundary.

TPF does not remove that difficulty, and it should not claim to. A pipeline can coordinate the typed flow that gathers facts, invokes policies, applies legitimate aggregate operations, and handles external consequences. It cannot declare an invariant and make it atomic across independent state stores, remote systems, or bounded contexts. The first design question remains domain ownership: which invariant must be protected locally, which rule is a policy that evaluates multiple facts, and which outcome can be eventually consistent?

For a local cross-aggregate policy, a focused domain service or specification can be the right home. It receives the necessary typed values, makes the rule explicit, and returns a result that aggregates can act upon. The pipeline’s job is then to arrange the use case: obtain facts through declared boundaries, invoke the policy, call aggregate behavior that preserves each aggregate’s invariants, and publish or persist an outcome in a controlled shell. The policy is business language; the pipeline is execution language.

For a distributed rule, the answer may include a reservation, a command, an event, an await boundary, or a compensation. The right choice depends on the business promise, not on the number of boxes in a diagram. TPF can make the boundary and operational semantics explicit. A checkpoint handoff is an ownership transfer: after admission, the downstream pipeline owns retry, DLQ, and lifecycle semantics. That clarity matters because a cross-aggregate flow otherwise leaves everyone responsible for failure and nobody responsible for recovery.

Stable identifiers and replay matter here. A retry across systems must preserve the idempotency, dispatch, checkpoint, or correlation identifiers that let a receiver recognise the same intent. A retry that creates a new business action on every attempt is not resilience; it is a discount generator with a pager. The shell makes these concerns systematic, while the domain still decides whether a second reservation is permitted.

The tempting bad answer is to put all rules in the pipeline because it sees the whole flow. That centralizes context but weakens local guarantees. The opposite bad answer is to deny that cross-aggregate policy exists because aggregates should be independent. That merely pushes the rule into a UI, a report, or an operator’s intuition. TPF gives a team a place to coordinate the rule without confusing coordination with ownership.

Some business guarantees must be stated honestly. A team may have to accept eventual consistency, design a compensating action, or choose a coarser aggregate where true atomicity is essential. No pipeline makes those choices disappear. It can prevent their implementation from being scattered across callbacks and invisible retries.

The mental model is: aggregates own invariants; policies express rules across facts; pipelines coordinate execution and external consequences. That allows a cross-aggregate flow to be complex without becoming mysterious.

## Trade-offs

TPF gains a clear coordination layer for policies that cross boundaries. It gives up the fantasy of universal atomicity. Teams must model ownership, idempotency, and compensation deliberately instead of expecting orchestration to solve a consistency problem by declaration.

## When TPF is not a good fit

If a rule truly requires one transactional consistency boundary, model it as such rather than distributing it optimistically through a pipeline. TPF is also not a replacement for a database transaction where one is appropriate and sufficient.
