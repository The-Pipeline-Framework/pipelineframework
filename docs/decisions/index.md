# Architectural Decisions

These records capture the durable choices that shape TPF. Each ADR answers one
coherent ownership question. Detailed design and authoring pages explain how to use
the resulting model; current source, compiler behaviour, tests, and current docs remain
authoritative for exact syntax and release support.

## Catalogue

| ADR | Decision |
| --- | --- |
| [0001](./0001-functional-core-and-explicit-dataflow.md) | Keep business transformations in a functional core and carry known facts explicitly. |
| [0002](./0002-compiler-owned-v3-type-model.md) | Make the compiler own v3 type, routing, and generated-contract semantics. |
| [0003](./0003-topology-cardinality-and-authored-units.md) | Choose topology and cardinality before Java service/operator shape. |
| [0004](./0004-nested-composition-and-bounded-recursion.md) | Compose pipelines as typed steps and express looping through bounded recursion. |
| [0005](./0005-query-is-external-observation.md) | Use Query only for genuinely new external observation. |
| [0006](./0006-command-owns-logical-effects.md) | Give Command and CommandEffectStore authority over logical external effects. |
| [0007](./0007-await-owns-durable-suspension.md) | Give Await transport-independent durable suspension semantics. |
| [0008](./0008-separate-state-and-replay-authorities.md) | Keep persistence, cache, capture, effects, Await, and execution state distinct. |
| [0009](./0009-payload-references-and-representations.md) | Carry large content by reference and separate canonical/local/durable/wire representations. |
| [0010](./0010-generated-boundaries-and-connectors.md) | Prefer compiler-generated boundaries and typed connectors over application glue. |
| [0011](./0011-runtime-deployment-configuration-and-telemetry.md) | Separate runtime layout, build topology, transport, configuration lifetime, and telemetry policy. |
| [0012](./0012-failure-recovery-and-handoff.md) | Keep business outcomes, item rejection, runtime recovery, effects, and handoff ownership distinct. |
| [0013](./0013-change-the-semantic-owner-boldly.md) | Change the owning abstraction coherently; impact determines rigor, not semantic placement. |
| [0014](./0014-remote-transition-outcome-uncertainty.md) | Keep remote caller timeout ambiguity under durable coordinator authority. |
| [0015](./0015-branch-routing-owns-await-projector-input.md) | Let v3 branch routing determine the typed request passed to an Await completion projector. |
| [0016](./0016-field-presence-nullability-and-compatibility.md) | Keep field presence and value nullability independent and classify evolution per compatibility surface. |
| [0017](./0017-structural-connector-execution-and-binding-serialization.md) | Declare blocking and serialized connector execution structurally and enforce serialization per binding and operation. |
| [0018](./0018-command-effects-use-immutable-durable-revisions.md) | Persist Command effect authority as immutable conditional revisions with lossless typed replay. |
| [0019](./0019-finite-streaming-query-reuses-one-to-many.md) | Express finite streaming Query through the existing ONE_TO_MANY runtime and deterministic child lineage. |
| [0020](./0020-query-capture-uses-immutable-durable-observation-revisions.md) | Persist Query observation authority as immutable conditional revisions with terminal streaming commit. |
| [0021](./0021-host-owned-connector-authentication.md) | Expose connector invocation semantics while leaving external authentication and credential resolution with the host. |
| [0022](./0022-hibernate-find-many-provider-strategy.md) | Preserve finite row streaming and declared total order in both Hibernate `find.many` providers. |
| [0023](./0023-hosted-llm-query-defaults-to-reactive-client.md) | Default hosted LLM Query to non-blocking provider I/O while retaining an explicit blocking implementation. |
| [0024](./0024-packaged-blocks-are-static-composition-imports.md) | Link packaged functional-core blocks as ordinary version 3 composition imports. |
| [0025](./0025-command-reissue-uses-occurrence-identity.md) | Give intentional Command reissue a new provider occurrence identity without weakening replay safety. |
| [0026](./0026-llm-clarification-is-inert-protocol-data.md) | Treat an LLM request for human information as inert typed protocol data. |
| [0027](./0027-mcp-imports-pinned-connector-operations.md) | Import selected MCP tools as pinned Connector operations without making MCP an Agent authority or runtime. |

## Maintenance

These files use the Nygard/MADR sections understood by Repowise: `Context`,
`Decision`, `Rationale`, and `Consequences`, with an explicit status. Keep one semantic
choice per file and name the affected repository areas concretely.

- `accepted` means current doctrine; `draft` means proposed; `rejected` or
  `deprecated` means it must not guide new work.
- Clarify an accepted decision in place when its meaning is unchanged.
- When direction changes, add a successor ADR, deprecate the old record, and record the
  supersession in Repowise rather than erasing the history.
- Update the relevant design/develop/deploy page and focused tests when exact behaviour
  changes. Do not put transient support matrices into these ADRs.
- Run `repowise update`, then inspect `repowise decision list --status all` and
  `repowise decision health` after meaningful decision changes.

Repowise is a decision index, not architectural authority. Confirm extracted or
historical records against current repository reality before relying on them.
