# Extending TPF with Operator Libraries

This guide is for teams publishing reusable operator libraries consumed by multiple pipelines.

## Library Contract

- Package operators in a regular dependency (`jar`) reachable from the consuming pipeline module.
- Expose public, non-abstract methods in `Class::method` format.
- Keep at most one input parameter per operator entry method.
- Avoid overloaded operator entry methods to keep resolution deterministic.

## Packaging Checklist

1. Publish the library artifact to your internal/external repository.
2. Add it as a dependency in the pipeline module whose pipeline YAML declares an `operator:` key (for example, `operator: "com.acme.lib.payment.PaymentOperators::enrich"`).
3. Ensure the operator classes are index-visible at build time: they must be discoverable in Jandex (for example, via `META-INF/jandex.idx`; see [Jandex Maven Plugin](https://github.com/wildfly/jandex/tree/main/maven-plugin)).
4. For instance methods, make classes CDI-manageable in the target runtime.

## Versioning Strategy

- Treat operator signatures as contracts.
- Use additive changes first (new methods/classes), then migrate YAML references.
- Coordinate breaking signature changes with pipeline updates in the same release.

## Transport Notes

- Operator category (`NON_REACTIVE`/`REACTIVE`) does not determine transport.
- REST paths can use direct domain JSON mapping.
- gRPC paths require protobuf descriptors and mapper-compatible bindings.

## Minimal Consumer Example

```yaml
steps:
  - name: "Enrich Payment"
    operator: "com.acme.lib.payment.PaymentOperators::enrich"
```

## Related

- [Developing with Operators](/guide/development/operators)
- [Operator Reuse Strategy](/guide/design/operator-reuse-strategy)
- [Operators](/guide/build/operators)
