# Synthetic Step Identifiers

Synthetic steps are generated from aspects. The runtime mapping uses a deterministic identifier, so placement is stable across builds.

## Canonical identifier

```text
<AspectId>.<Type>
```

- `AspectId`: the aspect class name or declared aspect id.
- `Type`: the synthetic type token (e.g., SideEffect, Validation, Observe, Cache).

Examples:

- `ObserveLatency.SideEffect`
- `Retry.Validation`

## Disambiguation suffix

If multiple synthetics exist for the same `<AspectId>.<Type>`, append a suffix:

```text
<AspectId>.<Type>@before
<AspectId>.<Type>@after
<AspectId>.<Type>@around
<AspectId>.<Type>@<index>
```

Examples:

- `ObserveLatency.SideEffect@before`
- `ObserveLatency.SideEffect@after`
- `ObserveLatency.SideEffect@2`

## Resolution rules

- If only a single synthetic exists for `<AspectId>.<Type>`, the unsuffixed id is valid.
- If multiple exist, the unsuffixed id is rejected with a clear error.
- `@<index>` is zero-based and stable based on the compiler expansion order.

## Versioning considerations

- The identifier format is stable and versionable.
- Renaming an aspect or changing its expansion order should be treated as a breaking change for mapping.
