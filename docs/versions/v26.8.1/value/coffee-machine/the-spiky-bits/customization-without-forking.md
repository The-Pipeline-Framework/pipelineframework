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
search: false
---

# Can generated code be customized without becoming archaeology?

## Elevator answer

**Change the template, provider, plugin, or adapter the generator reads. Editing `GeneratedPaymentHandler.java` works beautifully until Tuesday’s regeneration eats it.**

<CoffeeMisconceptions />

## The real explanation

Adding one header directly to `GeneratedPaymentHandler.java` feels efficient. Six months later generation removes it, production authentication fails, and Git blame points to a robot. Put the change in a declared adapter, provider, plugin, or template input so generated output remains disposable and the contract remains the source.

TPF should expose customization through declared configuration, connector implementations, plugins, templates, or other supported extension points. That keeps the variation visible, reviewable, and reproducible. If an extension point does not exist for a recurring need, that is evidence for evolving the model rather than normalizing edits to output.

The trade-off is that the supported path may be less immediate than a patch. It is also the only path that permits regeneration to remain a safety feature.

## Trade-offs

TPF gains reproducible customization. It gives up editing generated files as a shortcut.

## When TPF is not a good fit

If most required changes demand direct output edits, the model or generator is the wrong fit for that use case.
