# External interactions

Read this reference only for Query, Command, Await, model/browser, connector, correlation, or idempotency work. Verify exact syntax and support in current docs/source/tests.

## Query

Use Query for a genuinely new, current, or historical external observation: a database observation, provider status, or model proposal. If the pipeline already knew the fact, carry it instead.

Query capture replays the external observation; it is not generic cache or persistence. Treat model, browser, and connector results as untrusted observations. Keep authoritative identifiers, permissions, policy, and suspended request data in trusted pipeline/Await state. The framework should combine trusted context with the observation; never require the observer to echo authority correctly.

## Command

Email, payment, archive, indexing, ticket creation, and provisioning are logical Commands. The command ID identifies the logical effect. Execution, retry, dispatch, worker, and transport attempts are not new effect identities.

`CommandEffectStore` is the authority for recorded effect identity/outcome. The provider still needs a stable idempotency key or external identifier; TPF cannot make an arbitrary third party exactly-once after an ambiguous failure.

Keep provider connection/tuning in connector or runtime configuration and dynamic business input in the typed item. Operation configuration must not encode routing or sequencing.

## Await

Use Await when the final answer arrives later: human approval, webhook callback, brokered reply, or long-running provider result. Immediate request/response is Query/operator territory; cross-pipeline checkpoint handoff has separate downstream ownership.

Transport adapters may vary, but Await owns interaction/correlation identity, completion admission, timeout/deadline, duplicate completion, durable snapshots, and resume/continuation. Do not build an application polling table or workflow registry beside it.

Search current Query/Command/Await authoring docs first, then their compiler descriptors, runtime support, stores, and focused identity/replay/completion tests.
