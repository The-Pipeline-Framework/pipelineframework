# Testing policy across TPF repositories

The repository split changed where tests run; it did not reduce the testing contract.
Tests live with the repository that owns the behaviour, while the `pipelineframework`
coordination repository proves that independently published artifacts work together.

## Test ownership

| Behaviour under test | Owning repository |
| --- | --- |
| Semantic model, runtime contracts and serialization | `pipelineframework-contracts` |
| Compilation, validation and generated artifacts | `pipelineframework-compiler` |
| Quarkus/Spring runtime, stores and foundational plugins | `pipelineframework-runtime` |
| Connector, importer, representation-provider and host behaviour | `pipelineframework-connectors` |
| Packaged reusable pipeline definitions | `pipelineframework-blocks` |
| Distribution resolution and composition | `pipelineframework-expansions` |
| Learning proofs and narrowly scoped integration examples | `pipelineframework-examples` |
| Checkout/TPFGo, Search and QuickBooks production-shaped E2E paths | `pipelineframework-reference-implementations` |
| CSV/Kafka Payments and RAG application behaviour | Their standalone application repositories |
| Released compiler/runtime/connector interoperability | `pipelineframework`, in `framework/transport-completeness-tests` |

Moving a module without its owner-local tests and CI invocation is an incomplete extraction.

## Test layers

1. **Unit and contract tests** stay beside the type or semantic owner.
2. **Runtime integration and smoke tests** stay beside the runtime adapter or provider they exercise.
3. **Consumer tests** compile and run against released Maven coordinates, never sibling source trees.
4. **Cross-artifact compatibility tests** import `pipelineframework-bom` and exercise an exact released component set.
5. **Application E2E tests** stay with the application or reference implementation whose topology, assets and
   operational behaviour they prove.

The coordination compatibility module complements downstream E2E tests; it does not replace them.

## Naming and Maven lifecycle

- `*Test` runs with Surefire during `test`.
- `*IT` runs with Failsafe during `integration-test` and `verify`.
- Container-backed, native-image and topology tests may have dedicated workflow lanes, but their Maven ownership
  remains in the owning repository.
- Test selection must not create an alternate Maven reactor or source universe.
- `central-publishing` is the sole Maven profile exception and is publication-only.

## Cross-repository regressions

Every pull request must make its owner repository green. An upstream change is not considered compatible merely
because its own repository passes: it must also be exercised by the required downstream compatibility train using
fresh released snapshot artifacts. Until that train is automated, publish snapshots in dependency order and run
the affected downstream repositories explicitly.

See [Testing Guidelines](docs/evolve/testing-guidelines.md) for the policy and
[Repository Split Migration and Test Assessment](docs/evolve/repository-split-migration.md) for the current map,
audit findings and known gaps.

## Coordination-repository commands

Use the worktree-local Maven repository on every invocation:

```bash
./mvnw -f framework/pom.xml verify \
  -Dmaven.repo.local="$PWD/.m2/repository"

./mvnw verify \
  -Dmaven.repo.local="$PWD/.m2/repository"
```

These commands prove the coordination reactor only. Use the owner repository's `AGENTS.md`, build and workflows
for runtime, connector, example, reference-implementation and application validation.
