---
title: "Who decides connector, plugin, or step?"
faq:
  id: "connector-plugin-or-step"
  track: "connectors"
  question: "Who decides whether something is a connector, plugin, or pipeline step?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "enterprise-edna"
      text: "Everything is a plugin once it has configuration."
    - persona: "spring-sam"
      text: "Everything is a service once it can be injected."
    - persona: "platform-priya"
      text: "Everything is platform if it once caused an incident."
social:
  poll:
    question: "A plugin is for…"
    options:
      - "Business discounts"
      - "HTTP endpoints"
      - "Cross-cutting behavior"
      - "Classifying meetings"
    preferred: "Cross-cutting behavior"
fortune:
  quote: "A useful category answers who owns the consequence, not who wrote the class."
related:
- "connectors-not-stationery"
- "reactive-not-a-personality-test"
tags:
- "connectors"
- "connector"
- "plugin"
- "or"
- "step"
---

# Who decides connector, plugin, or step?

## Elevator answer

**Use a step for typed business behavior, a connector for external I/O boundaries, and a plugin for reusable cross-cutting runtime behavior such as telemetry or persistence.**

<CoffeeMisconceptions />

## The real explanation

This classification is not taxonomy for its own sake. It prevents three different responsibilities from being hidden in one convenient abstraction. A pipeline step contains typed business behavior: it transforms, decides, validates, or coordinates domain-relevant work. A connector represents an external admission or publication boundary. A plugin provides cross-cutting framework capability such as telemetry, caching, persistence integration, or logging.

The test is ownership. If a function’s main reason to exist is a domain decision, it belongs in a step or focused domain behavior. If it converts external reality into a typed contract or publishes outward, it is a connector. If it changes how many flows execute without becoming a business action itself, it is a plugin.

This protects the core. A Kafka client hidden in a business step makes a connector boundary invisible. A retry policy hidden in a connector makes a platform concern impossible to coordinate. A business discount hidden in a plugin makes the application’s meaning impossible to review. The framework’s categories are a way to expose those mistakes before they become conventions.

The trade-off is that unusual integrations may need a design discussion rather than a fast class name. That is appropriate when the code will determine who owns retries, credentials, durability, or business intent.

## Trade-offs

TPF gains clearer boundaries. It gives up a universal bucket called service.

## When TPF is not a good fit

If a team refuses to distinguish domain behavior from operational behavior, the model will feel restrictive because it is exposing a real disagreement.
