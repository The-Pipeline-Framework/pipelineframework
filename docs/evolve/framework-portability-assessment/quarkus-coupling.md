# Quarkus Coupling

Quarkus coupling appears in two forms.

- Deployment module uses Quarkus build items, build steps, generated bean registration, Jandex, and extension dependencies.
- Runtime module still has several Quarkus mechanisms that should be moved behind adapter seams.

| Runtime concern | Current mechanism | Portability issue | Proposed seam |
| --- | --- | --- | --- |
| Step bean lookup | `Arc.container().instance(...)` in `PipelineStepResolver` | Hard Quarkus container dependency | `BeanLookup` |
| Active profile | `LaunchMode.current().getProfileKey()` in `PipelineConfig` | Quarkus launch model | `RuntimeProfile` |
| Config mappings | SmallRye `@ConfigMapping` and MicroProfile `@ConfigProperty` | Different Spring binding model | `ConfigProvider` plus platform adapters |
| Local async work event | CDI `@ObservesAsync` in `PipelineExecutionService` | CDI event bus dependency | `EventBus` or `WorkDispatcher` |
| Bean collections | CDI `Instance<T>` | Different lookup and ordering model in Spring | `BeanLookup.list(type)` with priority metadata |
| Unremovable beans | `@Unremovable` | Quarkus Arc optimization concern | Quarkus adapter annotation only |

Quarkus remains the reference implementation while these concerns move to adapter seams.
