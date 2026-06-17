---
search: false
---

# Framework Portability Assessment

Snapshot: `origin/main@e1eda106`, assessed on 2026-06-07.

This guide captures what it would take to keep Quarkus as the existing runtime target while adding a Spring Boot portability path.

## Recommendation

TPF should keep Quarkus as the mature reference, extract a neutral core, and add Spring behind explicit renderer/runtime adapter seams.

## Guide Structure

- [Coupling inventory](/versions/v26.6.1/guide/evolve/framework-portability-assessment/coupling-inventory)
- [Quarkus coupling](/versions/v26.6.1/guide/evolve/framework-portability-assessment/quarkus-coupling)
- [Vert.x coupling](/versions/v26.6.1/guide/evolve/framework-portability-assessment/vertx-coupling)
- [Runtime split](/versions/v26.6.1/guide/evolve/framework-portability-assessment/runtime-split)
- [Reactive portability](/versions/v26.6.1/guide/evolve/framework-portability-assessment/reactive-portability)
- [Persistence portability](/versions/v26.6.1/guide/evolve/framework-portability-assessment/persistence)
- [Annotation removal](/versions/v26.6.1/guide/evolve/framework-portability-assessment/annotation-removal)
- [Code-generation portability](/versions/v26.6.1/guide/evolve/framework-portability-assessment/code-generation)
- [Maven and scaffolding](/versions/v26.6.1/guide/evolve/framework-portability-assessment/maven-and-scaffolding)
- [Roadmap and guardrails](/versions/v26.6.1/guide/evolve/framework-portability-assessment/roadmap-and-guardrails)

The highest-value first slice remains: behavior-preserving `runtime-core` extraction with Quarkus adapters still owning CDI, Quarkus config, transport bindings, and Vert.x context bridging.
