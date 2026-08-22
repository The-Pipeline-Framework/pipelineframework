---
title: "Can generated code be customized without becoming archaeology?"
faq:
  id: "customization-without-forking"
  track: "governance"
  question: "Can developers customize generated code without losing regeneration safety?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "codegen-carl"
      text: "Edit the generated file; it looks lonely."
    - persona: "framework-fred"
      text: "One small patch is not a fork."
    - persona: "enterprise-edna"
      text: "Copy it into a shared utility forever."
social:
  poll:
    question: "Customize via…"
    options:
      - "Generated edits"
      - "A fork"
      - "Extension points"
      - "Wishful comments"
    preferred: "Extension points"
fortune:
  quote: "Regeneration is safe only when the generated file is not secretly the source of truth."
related:
- "generation-and-diagnostics"
- "escape-hatch"
tags:
- "governance"
- "customization"
- "forking"
---

# Can generated code be customized without becoming archaeology?

## Elevator answer

**Customize through declared extension points and adapters, not edits to generated output; regeneration remains safe only when the contract remains the source of truth.**

<CoffeeMisconceptions />

## The real explanation

Generated output is a consequence of the pipeline model. Editing it directly may solve a local need once, but the next generation run cannot know whether to retain the change. The result is archaeology: the system works because a file was altered years ago, and nobody knows which contract it now represents.

TPF should expose customization through declared configuration, connector implementations, plugins, templates, or other supported extension points. That keeps the variation visible, reviewable, and reproducible. If an extension point does not exist for a recurring need, that is evidence for evolving the model rather than normalizing edits to output.

The trade-off is that the supported path may be less immediate than a patch. It is also the only path that permits regeneration to remain a safety feature.

## Trade-offs

TPF gains reproducible customization. It gives up editing generated files as a shortcut.

## When TPF is not a good fit

If most required changes demand direct output edits, the model or generator is the wrong fit for that use case.
