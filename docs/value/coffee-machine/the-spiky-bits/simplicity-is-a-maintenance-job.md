---
title: "Will this remain simpler than the architecture it replaces?"
faq:
  id: "simplicity-is-a-maintenance-job"
  track: "governance"
  question: "Why should we believe this framework will remain simpler than the architecture it replaces?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "framework-fred"
      text: "More abstractions always simplify."
    - persona: "consultant-nigel"
      text: "Existing complexity is free because it is familiar."
    - persona: "consultant-nigel"
      text: "A reference architecture cannot age."
social:
  poll:
    question: "Simplicity needs…"
    options:
      - "More layers"
      - "Blind faith"
      - "Evidence"
      - "A new acronym"
    preferred: "Evidence"
fortune:
  quote: "A framework remains simple only when it continues to delete more accidental complexity than it introduces."
related:
- "startup-size"
- "escape-hatch"
tags:
- "governance"
- "simplicity"
- "maintenance"
- "job"
---

# Will this remain simpler than the architecture it replaces?

## Elevator answer

**You should not believe it automatically; TPF stays worthwhile only while its explicit contracts remove more repeated accidental complexity than its model, tooling, and governance add.**

<CoffeeMisconceptions />

## The real explanation

No framework remains simple by declaration. TPF adds concepts: pipelines, connectors, plugins, generated artifacts, runtime mappings, and contracts. It earns those concepts only when they replace larger repeated costs—hidden I/O, accidental retries, ambiguous ownership, inconsistent adapters, and failures discovered too late.

The test is practical. Can a team explain a business flow, locate its external boundaries, understand failure ownership, evolve a mapping, and investigate an incident more easily than before? Are generated artifacts deterministic and reviewable? Are escape hatches rare and explicit? If the answer becomes no, the framework must be simplified or its use narrowed.

This requires maintenance: remove obsolete conventions, resist adding a global abstraction for every exception, preserve good diagnostics, and keep extensions disciplined. Complexity does not disappear; it must be placed where many teams can share the cost instead of rediscovering it independently.

## Trade-offs

TPF gains shared structure. It gives up the right to be trusted forever without evidence.

## When TPF is not a good fit

If the model, generation, and governance cost more than the operational complexity they remove, use ordinary application code.
