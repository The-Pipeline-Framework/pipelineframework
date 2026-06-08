# Reactive Portability

Three migration options for async stack:

- Keep Mutiny internally (lowest disruption, Quarkus-friendly, Spring via adapters).
- Migrate fully to Reactor (high disruption, no immediate benefit for Quarkus users).
- Add staged library neutrality (best long-term if done as adapter layer).

Recommended path is staged neutrality: keep current Mutiny contracts and add Reactor adapters where Spring needs them.

For core store/contract boundaries, prefer `CompletionStage<T>` for unary and reactive-streams `Publisher<T>` for streams.

The adaptation cost is significant because core execution classes are built around Mutiny operators (`retry`, `transform`, subscription, backpressure, failure handling). `PipelineStepExecutor`, `QueueAsyncCoordinator`, `AwaitStepSupport`, and telemetry paths need explicit semantics before adaptation.
