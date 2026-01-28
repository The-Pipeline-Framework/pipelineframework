---
search: false
---

# Business Value

The Pipeline Framework is designed to accelerate delivery without locking you into a proprietary runtime.

## Why Teams Choose It

1. **Speed of Development**: Visual pipeline design produces runnable services in minutes.
2. **Microservices-in-a-Box**: Each step is a standalone service, with adapters generated at build time.
3. **Lower Risk of Lock-in**: Plain Java, Quarkus, and standard gRPC keep the stack portable.
4. **Operational Clarity**: Append-first persistence improves traceability and auditability when enabled.

## Observed Impact (CSV Payments)

In the CSV Payments example, a 10–12 person team spent roughly 6 months on a prior implementation. The result was materially weaker in design quality, maintainability, extensibility, operability, and overall feature coverage compared to the current TPF-based implementation.

This is not a controlled study, and process differences always exist across organizations. Still, the gap was significant enough to treat TPF as a structural accelerator, not just a tooling improvement.

## Expected Outcomes

Teams typically see:

1. **Delivery time reductions** in the 10x range for comparable scopes
2. **Lower cost of change** due to generated adapters and consistent conventions
3. **Higher operational readiness** via built-in observability and traceability

Large enterprises vary in process maturity, but the core productivity gains come from eliminating manual glue code and keeping services focused on the single `process()` contract.

## What It Enables

1. **Faster Iteration**: Change steps independently without reworking the entire pipeline.
2. **Predictable Scaling**: Each step scales on its own workload characteristics.
3. **Measurable ROI**: Reduced boilerplate, shorter lead times, and less custom glue code.
