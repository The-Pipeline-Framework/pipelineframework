# POM vs Layout Matrix

This page maps runtime layout intent to concrete Maven topology in `examples/csv-payments`.

Use it to answer two questions quickly:

1. What does `pipeline.runtime.yaml` change logically?
2. Which POM/module topology is required to make that shape real?

## Canonical model

- **Runtime layout** = logical placement (`pipeline.runtime.yaml`).
- **Build topology** = physical deployables (Maven modules/POMs).
- **Transport** = call mechanism (`GRPC`, `REST`, `LOCAL`).

If layout and topology are not aligned, the build can succeed but runtime behavior will not match the intended deployment shape.

## Layout to topology matrix (CSV Payments)

| Target runtime layout | Runtime mapping file | Maven entrypoint | Physical deployables produced | Status in csv-payments |
| --- | --- | --- | --- | --- |
| `modular` | `config/runtime-mapping/modular-auto.yaml` or `modular-strict.yaml` | `examples/csv-payments/pom.xml` | Per-service runtimes + orchestrator + persistence | Implemented |
| `pipeline-runtime` | `config/runtime-mapping/pipeline-runtime.yaml` | `examples/csv-payments/pom.pipeline-runtime.xml` via `build-pipeline-runtime.sh` | `orchestrator-svc` + `pipeline-runtime-svc` + `persistence-svc` | Implemented |
| `monolith` | `config/runtime-mapping/monolith.yaml` | `examples/csv-payments/pom.monolith.xml` via `build-monolith.sh` | Single `monolith-svc` runtime | Implemented |

## Phase/execution relevance by topology

Legend:
- `required`: needed for that topology
- `n/a`: not part of that topology
- `depends`: required only if that module is present in the topology

| Build concern | Modular (`pom.xml`) | Pipeline-runtime (target) | Monolith (`pom.monolith.xml`) |
| --- | --- | --- | --- |
| Parent dev cert generation (`generate-dev-certs.sh`) | required | required | required |
| Per-module role source roots (`target/generated-sources/pipeline/*`) | required | required | required |
| Role-specific compile outputs (`classes-pipeline/*`) | required | required | required |
| Merge role outputs into `target/classes` before packaging | required | required | required |
| Source aggregation from multiple service modules | n/a | required for grouped runtime module | required (`monolith-svc`) |
| Standalone orchestrator runtime module | required | required | n/a |
| Standalone grouped pipeline runtime module | n/a | required (`pipeline-runtime-svc`) | n/a |
| Standalone plugin runtime module (for plugin aspects) | depends | required (typical) | n/a |

## Practical interpretation

- Runtime mapping alone can make placement deterministic, but not alter artifact count.
- Build topology determines whether you actually deploy 1, 2, or many runtimes.
- CSV Payments now ships dedicated topology lanes for all three layouts:
  modular (`pom.xml`), pipeline-runtime (`pom.pipeline-runtime.xml`), and
  monolith (`pom.monolith.xml`).

## Related pages

- [Runtime Layouts and Build Topologies](/guide/build/runtime-layouts/)
- [Using Runtime Mapping](/guide/build/runtime-layouts/using-runtime-mapping)
- [Maven Migration Playbook](/guide/build/runtime-layouts/maven-migration)
- [CSV Payments Pipeline-Runtime Walkthrough](/guide/build/runtime-layouts/csv-payments-pipeline-runtime)
- [CSV Payments Monolith Walkthrough](/guide/build/runtime-layouts/csv-payments-monolith)
- [Pipeline Parent POM Lifecycle](/guide/build/pipeline-parent-pom-lifecycle)
- [CSV Payments POM Lifecycle](/guide/build/csv-payments-pom-lifecycle)
