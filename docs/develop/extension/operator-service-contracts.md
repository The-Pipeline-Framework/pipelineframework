# Operator Service Contracts

Use this path when a reusable operator owns its own external DTO/entity model and the pipeline should keep a separate domain contract.

## Boundary Shape

```text
Application domain type
  -> operator mapper
Operator DTO/entity type
  -> operator transport mapper
Transport or local call
```

The mapper is the important boundary. It prevents external provider shape from leaking into the business flow.

## Use This When

- the operator is reused across applications,
- the operator model differs from the application domain model,
- the operator may later move from local to remote transport,
- pair-accurate mapper selection enables proper mapper pair validation.

For implementation details, see [Operator Delegation Reference](/develop/extension/operator-delegation-reference#option-2--use-domain-types).
