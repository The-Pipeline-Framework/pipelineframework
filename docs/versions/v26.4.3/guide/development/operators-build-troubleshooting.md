---
search: false
---

# Operator Build Troubleshooting

Use this matrix for compile-time/operator-resolution failures.

## Build and CI Signatures

| Signature (exact/close match) | Likely Cause | First Actions |
| --- | --- | --- |
| `Class not found` / `method not found` during operator resolution | Operator class not on build classpath, wrong FQCN, or wrong method name | Verify dependency graph, package name, and `operator: Class::method` value in YAML |
| `More than 1 matching method` / overloaded method ambiguity | Operator method overloading unsupported for resolved path | Rename target method or make operator entrypoint unique |
| `Method has more than 1 parameter` | Operator contract mismatch with invoker expectations | Refactor to single input parameter (or unary input shape) |
| `gRPC transport requires protobuf descriptors` | Descriptor set unavailable for gRPC delegated/operator path | Verify descriptor generation and build inputs for gRPC modules |
| `... requires a mapper ...` for gRPC delegated/operator step | Missing or non-matching mapper binding for transport path | Add/fix mapper pair and ensure binding generation matches routing conventions |
| `Build step ... does not produce any build item` | Quarkus build step wiring issue | Check `@BuildStep` producer/consumer contract and `@Produce` usage |

## Recommended Verification Order

1. Reproduce with the narrowest command first.
2. Fix the contract issue in YAML/classpath/mappers.
3. Re-run targeted module tests.
4. Expand to full `verify` only when targeted checks are green.

## CI-Equivalent Commands

```bash
./mvnw verify
./mvnw -f framework/pom.xml verify
```

### Optional Reference Lane (Search Example)

```bash
./mvnw -f examples/search/pom.xml -pl orchestrator-svc -am \
  -Dpipeline.platform=FUNCTION \
  -Dpipeline.transport=REST \
  -Dpipeline.rest.naming.strategy=RESOURCEFUL \
  -DskipTests compile
```

## Related

- [Developing with Operators](/versions/v26.4.3/guide/development/operators)
- [Operator Troubleshooting Matrix](/versions/v26.4.3/guide/operations/operators-troubleshooting)
