# Validation, Migration, and Examples

## Validation rules

- Unknown step id or synthetic id in the mapping is an error.
- Duplicate mapping for a step or synthetic is an error.
- Unknown module or runtime name is an error.
- In `validation: strict`, every step and synthetic must be placed.
- If multiple synthetics share the same `<AspectId>.<Type>`, an unsuffixed id is an error.
- In `layout: monolith`, all placements must resolve to the monolith module.
- In `layout: pipeline-runtime`, each pipeline's steps must resolve to a single runtime.
- Transport is configured in `pipeline.yaml` and is not overridden here.

## Error messaging strategy

Errors should be actionable and deterministic, for example:

- `RUNTIME_MAP_UNKNOWN_STEP`: Step "ValidateCard" not found in pipeline.
- `RUNTIME_MAP_MISSING_STEP`: Step "ValidateCard" has no module assignment in strict mode.
- `RUNTIME_MAP_DUPLICATE_STEP`: Step "ValidateCard" mapped to modules "A" and "B".
- `RUNTIME_MAP_UNKNOWN_MODULE`: Module "payments-core" not declared.
- `RUNTIME_MAP_SYNTHETIC_AMBIGUOUS`: "ObserveLatency.SideEffect" has 2 instances; use @before/@after/@<index>.

For `RUNTIME_MAP_SYNTHETIC_AMBIGUOUS`, suffixes select a specific synthetic instance:
`@before` means the synthetic attached before the target step, `@after` means after it,
and `@<index>` is the zero-based expansion position. See
[`Synthetic Step Identifiers`](./synthetics.md#disambiguation-suffix).

```yaml
synthetics:
  ObserveLatency.SideEffect@before:
    module: cache
  ObserveLatency.SideEffect@1:
    module: persistence
```

## Migration plan

1. Add `pipeline.runtime.yaml` with only `version` and `validation: auto`.
2. Add runtimes/modules as needed, without step mappings.
3. Map steps in batches (starting with non-controversial groupings).
4. Map synthetics only when needed (default plugin grouping is usually fine).
5. Switch to `validation: strict` once all mappings are explicit.

## Practical heuristics

- Image size and heap baseline dominating? Start with `pipeline-runtime` or `monolith`.
- Teams deploying independently? Stay `modular` and only map hot spots.
- Orchestrator as a control point? Keep it as a separate runtime even when collapsing services.
## Examples

### 1) Simple pipeline (defaults only)

```yaml
version: 1
validation: auto
```

### 2) csv-payments style grouping

```yaml
version: 1
layout: modular
validation: auto

defaults:
  runtime: local
  module: per-step
  synthetic:
    module: plugin

runtimes:
  local: {}
  payments: {}

modules:
  payments-core:
    runtime: payments
  persistence:
    runtime: local
  cache:
    runtime: local

steps:
  ParseCsv:
    module: payments-core
  ValidateCard:
    module: payments-core
  CaptureFunds:
    module: payments-core
  PersistPayment:
    module: persistence

synthetics:
  ObserveLatency.SideEffect:
    module: cache
```

### 3) Majestic monolith

```yaml
version: 1
layout: monolith
validation: auto

runtimes:
  monolith:
    # orchestrator: in-process (implicit in monolith layout)

modules:
  monolith:
    runtime: monolith
    # Orchestrator is bundled here and uses in-process wiring.
```
