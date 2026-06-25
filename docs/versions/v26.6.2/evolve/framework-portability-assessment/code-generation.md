---
search: false
---

# Code Generation Portability

Current generation is model-aware but renderer assumptions are Quarkus-centric.

Current model:

```mermaid
flowchart LR
    A["YAML + annotations"] --> B["Semantic model"]
    B --> C["Target resolution"]
    C --> D["Bindings"]
    D --> E["Hardcoded renderer set"]
    E --> F["CDI/JAX-RS/gRPC/Mutiny artifacts"]
```

Target model:

```mermaid
flowchart LR
    A["YAML + optional annotations"] --> B["Semantic model"]
    B --> C["Target resolution"]
    C --> D["Bindings"]
    D --> E{"Renderer profile"}
    E --> F["Quarkus renderer set"]
    E --> G["Spring renderer set"]
    F --> H["CDI + RESTEasy Reactive + Mutiny"]
    G --> I["Spring beans + WebFlux + Reactor"]
```

Renderer assumptions to split:

- Bean scope and injection (`@ApplicationScoped`/`@Inject` vs Spring equivalents)
- REST transport (`JAX-RS` vs WebFlux)
- Reactive types (`Uni`/`Multi` vs `Mono`/`Flux`)
- Blocking/offload policy (`@RunOnVirtualThread` vs scheduler policy)
- Context propagation (Vert.x locals vs Reactor context)

The semantic IR remains transport/platform-agnostic; renderer profile owns framework assumptions.
