---
title: "Can Kafka stay while the flow changes?"
faq:
  id: "kafka-can-stay"
  track: "bring-your-existing-app"
  question: "Can we keep our current Kafka consumers and producers during migration?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "kafka-frank"
      text: "A topic is the domain model after enough consumers depend on it."
    - persona: "spring-sam"
      text: "Inject the producer into every service so no event feels lonely."
    - persona: "platform-priya"
      text: "Move the broker first; meaning can follow during the incident."
social:
  poll:
    question: "What changes first in a Kafka migration?"
    options:
      - "Every topic name"
      - "The broker cluster"
      - "Explicit typed boundary"
      - "The pager rotation"
    preferred: "Explicit typed boundary"
fortune:
  quote: "Kafka can remain the road; the domain does not need to become the traffic report."
related:
- "jpa-is-not-a-crime"
- "migrate-one-capability"
tags:
- "bring-your-existing-app"
- "kafka"
- "can"
- "stay"
---

# Can Kafka stay while the flow changes?

## Elevator answer

**Yes. Keep Kafka adapters at the edge, migrate typed business behavior inward, and make ownership, mapping, retries, duplicates, and backpressure explicit gradually.**

<CoffeeMisconceptions />

## The real explanation

Kafka is usually both infrastructure and history. Consumers, producers, topics, offsets, and established operational conventions often carry production traffic before a framework migration begins. Treating all of that as disposable would be reckless. TPF allows Kafka to remain at the boundary while a team changes the business flow behind it.

A Kafka consumer can continue admitting a record, using the existing serialization and operational setup. The migration seam is the translation from the external record into a typed input and the declared flow that follows. On the way out, a declared, versioned connector boundary can publish the required external representation while domain events and business results retain their own meaning. This is not Kafka denial; it is refusing to make topic names, schemas, and offsets the vocabulary of every business decision.

The gradual approach is important for delivery semantics. Existing duplicate handling, partition order, consumer groups, DLQ practice, and backpressure are part of the real system. A new pipeline must not claim exactly-once processing merely because it has a typed contract. The logical identity of an effect must remain stable across retries even though Kafka may make several delivery attempts. That distinction supports idempotency, correlation, explicit ownership, and honest duplicate tests. TPF can make those obligations visible; it cannot repeal broker behavior.

Teams can migrate one consumer path at a time, keep established producers, and compare results. As confidence grows, mapping, connector versions, representations, telemetry, and retry semantics become part of the boundary contract. The legacy adapter may stay permanently if Kafka remains the right transport. The goal is not to eliminate the broker; it is to keep its mechanics at the edge where they can be operated honestly.

The trade-off is transitional complexity. For a while, the old consumer and the new flow must agree about acknowledgements, retries, and failures. That requires careful ownership, not a generic abstraction that hides offsets until an incident demands them.

## Trade-offs

TPF gains typed business behavior without requiring broker replacement. It gives up a little local convenience: Kafka-specific behavior must be declared and owned at the boundary rather than leaking through every step.

## When TPF is not a good fit

If the entire value of a component is Kafka-specific stream processing, a pipeline may add little. Use TPF where Kafka carries a business flow that needs richer execution semantics, not where Kafka itself is the product.
