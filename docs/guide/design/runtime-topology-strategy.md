# Runtime Topology Strategy

This guide is for software architects choosing how a pipeline is deployed.

## Decision model

Choose topology by balancing:

- Change isolation (blast radius)
- Operational footprint (containers/charts)
- Latency budget (network hops vs in-process calls)
- Security boundaries (public ingress and service exposure)
- Team ownership boundaries

## Topology trade-offs

| Topology | Strengths | Risks |
| --- | --- | --- |
| `modular` | Strong isolation, clear ownership, independent scaling | Highest operational overhead |
| `pipeline-runtime` | Good compromise: isolated orchestrator + grouped service runtime | Some coupling inside grouped runtime |
| `monolith` | Lowest deploy complexity and network latency between steps | Largest blast radius and least isolation |

## Orchestrator boundary

Keeping orchestrator isolated is usually a good default when you want:

- One external ingress point
- Reduced attack surface for internal services/plugins
- Stable migration path from grouped runtime to more modular layouts

## Plugin/synthetic placement

Treat plugin side effects as first-class when designing topology. They are easy to forget and can silently add coupling or load to the wrong runtime.

Practical rule:

- If plugins are operationally independent concerns (persistence/cache/observability), keep their placement explicit in runtime mapping.
- In monolith mode, accept that plugins collapse into the same blast radius as core steps.

## Migration architecture

Recommended evolution path:

1. `pipeline-runtime` as default operational baseline.
2. Split hotspots from grouped runtime into dedicated modules/runtimes when needed.
3. Keep runtime mapping and Maven topology synchronized at each step.

This avoids "logical layout only" drift where YAML says one thing but build artifacts enforce another.
