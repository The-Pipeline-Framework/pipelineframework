---
search: false
---

# Legacy Delegation Migration

Older TPF examples often describe delegation as annotation-driven Java wiring. Current authoring should make the pipeline contract YAML-first.

## Migration Steps

1. Keep `@PipelineStep` only for internal execution services that still need that marker.
2. Move step order, operator references, cardinality, and boundary declarations into YAML.
3. Replace hidden adapter glue with explicit operators or mappers.
4. Run build-time validation before changing deployment topology.

## Before

```java
@PipelineStep
public Output run(Input input) {
    return legacyLibrary.call(input);
}
```

## After

```yaml
steps:
  - name: call-legacy-library
    operator: com.example.LegacyLibrary::call
    input: Input
    output: Output
```

For the full legacy reference, see [Operator Delegation Reference](/versions/v26.7.1/develop/extension/operator-delegation-reference#migration-guide).
