---
title: "What happens to controllers that already work?"
faq:
  id: "controllers-are-not-the-enemy"
  track: "bring-your-existing-app"
  question: "Do we have to rewrite our REST controllers into pipelines?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "spring-sam"
      text: "A controller is a domain service after it has enough annotations."
    - persona: "spring-sam"
      text: "If HTTP status codes leave the controller, the business has gained protocol literacy."
    - persona: "enterprise-edna"
      text: "Move the controller into a package called pipeline and declare the migration complete."
social:
  poll:
    question: "What should a controller own?"
    options:
      - "Every rule that arrives"
      - "The whole distributed system"
      - "Typed business flow"
      - "Nothing; delete the web"
    preferred: "Typed business flow"
fortune:
  quote: "A controller is a useful doorway; it should not have to be the whole building."
related:
- "keep-spring-boot"
- "migrate-one-capability"
tags:
- "bring-your-existing-app"
- "controllers"
- "enemy"
search: false
---

# What happens to controllers that already work?

## Elevator answer

**No. Keep the route, request binding, authentication, and status codes. Move the part that prices the order, charges the card, and emails the receipt into a typed flow.**

<CoffeeMisconceptions />

## The real explanation

The controller is not guilty. It was standing nearest to the HTTP request when everybody started giving it responsibilities.

A healthy controller knows that `POST /orders` needs authentication, that malformed JSON gets a 400, and that an accepted order becomes a 202. Trouble starts when the same method loads the customer, checks credit, reserves stock, calls the payment API, retries it, publishes to Kafka, and decides which exception means 409. At that point the doorway has become the whole building.

TPF lets the doorway stay. Map the request to a typed input, admit it to the flow, then map the result back to HTTP. The pricing rule sees an order, not a header. The payment step returns a business result, not `ResponseEntity`. If it needs today's exchange rate, make that fresh observation a Query; do not have the controller reconstruct half a previous execution from database rows.

Migration can start without changing the route at all. Extract one decision, have the existing controller call it, and keep the current exception mapping while characterization tests compare responses. When the same flow later starts from Kafka or gRPC, those adapters translate their own envelopes into the same typed input. REST, gRPC, and LOCAL are still different transports; the discount rule simply stops caring which one woke it up.

The seam does need policing. Request-shape validation belongs at the web edge; rules such as “a cancelled order cannot be paid” belong in the flow. Copying the same rule into both places buys two future bugs for the price of one.

## Trade-offs

TPF keeps the familiar web adapter and makes the business flow callable from somewhere else. The cost is an explicit mapping and a real decision about where validation lives.

## When TPF is not a good fit

For a simple local endpoint with no meaningful operational boundary, a controller and focused service may be clearer. Do not create a pipeline only because the route has a verb in it.
