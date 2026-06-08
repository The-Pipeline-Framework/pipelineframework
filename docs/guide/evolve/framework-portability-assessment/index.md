# Framework Portability Assessment

Snapshot: `origin/main@e1eda106`, assessed on 2026-06-07.

This guide captures what it would take to keep Quarkus as the existing runtime target while adding a Spring Boot portability path.

## Recommendation

TPF should keep Quarkus as the mature reference, extract a neutral core, and add Spring behind explicit renderer/runtime adapter seams.

## Guide Structure

- [Coupling inventory](/guide/evolve/framework-portability-assessment/coupling-inventory)
- [Quarkus coupling](/guide/evolve/framework-portability-assessment/quarkus-coupling)
- [Vert.x coupling](/guide/evolve/framework-portability-assessment/vertx-coupling)
- [Runtime split](/guide/evolve/framework-portability-assessment/runtime-split)
- [Reactive portability](/guide/evolve/framework-portability-assessment/reactive-portability)
- [Persistence portability](/guide/evolve/framework-portability-assessment/persistence)
- [Annotation removal](/guide/evolve/framework-portability-assessment/annotation-removal)
- [Code-generation portability](/guide/evolve/framework-portability-assessment/code-generation)
- [Maven and scaffolding](/guide/evolve/framework-portability-assessment/maven-and-scaffolding)
- [Roadmap and guardrails](/guide/evolve/framework-portability-assessment/roadmap-and-guardrails)

The highest-value first slice remains: behavior-preserving `runtime-core` extraction with Quarkus adapters still owning CDI, Quarkus config, transport bindings, and Vert.x context bridging.
