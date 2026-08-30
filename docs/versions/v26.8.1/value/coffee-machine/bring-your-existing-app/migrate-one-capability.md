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
      - "Everything after a long debate"
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
search: false
---

# Can we move one capability without beginning a rewrite cult?

## Elevator answer

**Yes. Keep the controller, repository, or consumer at the edge; move one painful flow behind it; and make every new TPF piece earn its keep by deleting old plumbing.**

<CoffeeMisconceptions />

## The real explanation

Pick something a developer can point at: the 180-line method that loads an order, calls a fraud API, retries payment, saves status, and sends Kafka at the end. Leave the controller or consumer in place. First extract the pricing decision and make the old method call it. Then move the fresh fraud observation to a Query. Then move the charge to a Command. Compare behavior after each cut.

This is a strangler with manners. The old path remains evidence while the new one proves itself. Existing repositories and exception translation may stay temporarily, provided each bridge has an owner and a replacement decision. Ideological neatness is a poor substitute for knowing why production returns 409 in one obscure case.

Watch the deletion side of the diff. A generated adapter should replace a hand-written adapter. A runtime retry policy should retire the local retry loop. A typed mapping should remove the mapper chain it supersedes. A migration that only adds files is not migrating. It is collecting architectures.

TPF compilation can reject incompatible step mappings, missing connector declarations, and unsupported flow shapes before deployment. It cannot tell you that the old controller quietly waived a fee for customers in Malta. Characterization tests and comparison runs carry that part of the promise.

Mixed styles are the price of moving safely. Keep the seam narrow, write down what remains on each side, and turn every proven replacement into deletion or deliberate long-term ownership. Otherwise “incremental” becomes the polite word for “both forever.”

## Trade-offs

TPF gains a reversible path with evidence at every cut. The cost is temporary bridges, comparison tests, and the discipline to delete the plumbing each new capability replaces.

## When TPF is not a good fit

If no capability has a stable boundary or the team cannot invest in comparative tests, pause before migration. TPF does not turn an unowned legacy system into a safe change merely by adding YAML.
