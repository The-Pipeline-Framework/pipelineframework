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
search: false
---

# Can Kafka stay while the flow changes?

## Elevator answer

**Yes. Keep the consumer group, topics, schemas, and pager runbook. Translate each record into typed flow data, and keep offsets out of the invoice.**

<CoffeeMisconceptions />

## The real explanation

Kafka can remain Kafka. The invoice does not need to learn what an offset is.

Keep the current consumer, deserializer, group ID, and operational setup. At the seam, turn the `InvoiceRequested` record into a typed flow input. Inside the flow, steps talk about the invoice, customer, and payment—not topic names and acknowledgement modes. On the way out, a declared connector maps the business result to the versioned record the existing producer must publish.

None of this repeals broker physics. A record can arrive twice. Partition order still matters. The DLQ still exists, and backpressure does not disappear because the pipeline compiled. If publishing a payment result is retried after a lost response, the Command needs a stable logical effect identity so recovery can tell “same effect, another attempt” from “charge the card again.” Exactly-once is not a personality trait acquired by adding types.

Move one consumer path at a time. Run duplicate, timeout, rebalance, and poison-record cases against the old and new paths. Decide precisely when the record is acknowledged and who owns a failure before and after that point. The legacy Kafka adapter may stay forever if it is still the right road; the useful migration is that broker mechanics stop leaking into every business class.

During coexistence, the old consumer and new flow must agree about acknowledgements, retries, and failures. Write that agreement down where an operator can find it. An abstraction that hides offsets until an incident is just an incident with a nicer class name.

## Trade-offs

TPF gains typed business behavior without replacing the broker. The cost is declaring mapping, acknowledgement, duplicate, and retry behavior at the Kafka boundary.

## When TPF is not a good fit

If the entire value of a component is Kafka-specific stream processing, a pipeline may add little. Use TPF where Kafka carries a business flow that needs richer execution semantics, not where Kafka itself is the product.
