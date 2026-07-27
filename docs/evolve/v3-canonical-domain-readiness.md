# V3 Canonical Domain Readiness

This internal assessment defines the honest release boundary for the version 3 canonical-domain model. It records the verified baseline, rather than promising the future representation-provider platform.

## Release decision

Release v3 as an **experimental preview now**. Release Gate A is evidenced by a clean CSV Payments pipeline-runtime image build and end-to-end execution under `-Dcsv.v3.persistence=true`: OpenCSV input becomes a canonical `PaymentRecord`, persistence uses its explicit external representation, await sends protobuf and resumes a canonical `PaymentStatus`, canonical branches execute, and the terminal merge/publish path completes. The generated canonical model, generated protobuf contracts and adapters, explicit persistence mapping, OpenCSV provider boundary, and committed-IDL lifecycle are verified. Gate B items are not preview blockers. Gate C now has a provider-platform reference implementation; broader provider migration and non-Java targets remain future work.

The preview contract is opt-in through `version: 3`. Version 2 remains frozen, and a generated v3 pipeline must not mix v2 business contracts with its generated canonical contracts.

## Feature status

| Feature | Implemented | Tested | Documented | Status | Known limitation | Preview blocker |
| --- | --- | --- | --- | --- | --- | --- |
| Product records | Generated immutable Java records | Generator round-trip tests | Yes | SUPPORTED | Java target only | No |
| Nominal wrappers | Generated records with constructor constraints | Constraint and round-trip tests | Yes | SUPPORTED | Portable pattern profile is not defined | No |
| Aliases | Resolved transparently | Loader and generator tests | Yes | SUPPORTED | No generated alias class | No |
| Constraints | Generated Java constructor invariants | Generator and compatibility tests | Yes | SUPPORTED | Java regex is target-specific | No |
| Discriminated unions | Sealed Java API and authored discriminators | Union adapter and routing tests | Yes | SUPPORTED | Inline/payload-less variants are deferred | No |
| Protobuf generation | Shared types proto, IDL lock, and adapters | IDL compatibility and generation tests | Yes | SUPPORTED | Protobuf remains the only generated transport target | No |
| Java generation | Domain records, wrappers, unions, and adapters | Generated-source compile/round-trip tests | Yes | SUPPORTED | Public generated API remains experimental | No |
| Local unary execution | Generated adapters used for exact canonical Java signatures | Compiler and CSV proof | Yes | SUPPORTED | Exact generated types are required | No |
| Branch planning and terminal merge | Union-aware canonical payload applicability and terminal union wrapping | Planner, runtime, and CSV proof | Yes | SUPPORTED | Type-based only; no discriminator predicates | No |
| Await | Canonical step contract with generated protobuf transport metadata and adapters | Descriptor compatibility, transport, resume, and CSV Kafka await tests | Yes | SUPPORTED | Current proof covers the generated protobuf transport path | No |
| Persistence mapping | Explicit record-to-representation conversion before write | Compiler and persistence tests | Yes | SUPPORTED | Record writes only; query/read support is deferred | No |
| OpenCSV mapping | Provider-generated canonical facade over an explicit row `Mapper<Canonical, Row>` | Provider, CSV reader, mapper, and E2E tests | Yes | SUPPORTED | The provider owns OpenCSV semantics; core remains unaware of row types, keys, conventions, and rendering | No |
| JSON | Existing Jackson/protobuf JSON paths | Existing runtime tests | Partial | EXPERIMENTAL | No direct generated-domain JSON policy or union support | No |
| Generated gRPC client execution | Canonical public step signatures with protobuf stub calls | Renderer tests and CSV pipeline-runtime proof | Yes | EXPERIMENTAL | CSV proof is unary; broader deployment paths remain to be qualified | No |
| Non-unary gRPC cardinalities | Canonical public contracts around protobuf transport for all four shapes | Focused generated-source tests | Yes | EXPERIMENTAL | No assembled non-unary v3 application proof yet | No |
| App-generator integration | Existing scaffolding remains | Separate repository tests | Partial | DEFERRED | Deterministic DTO/mapper ownership has not migrated | No |
| Other language targets | Target-neutral model preserved | N/A | Yes | DEFERRED | Java is the only canonical-domain generator | No |

## Representation mapping contract

`RepresentationMapping` is a component-neutral declaration indexed by canonical domain type and component key. Its representation type and mapper type are optional at the model level. The normalized model accepts mappings for records, wrappers, aliases, and unions; a consumer owns the capability decision and must name the domain type and key when it rejects a shape.

The mapping conformance tests cover declared and absent lookup, optional class names, mapping identity, every named v3 type kind, duplicate-key rejection, and the invariant that mappings do not alter the IDL snapshot. Persistence additionally validates classpath availability and the exact `Mapper<Domain, Representation>` pair before it generates the boundary.

## Representation identity versus contract identity

An await step carries two deliberately separate type identities:

- **Contract identity** (`inputType` and `outputType`) is the canonical generated-domain type used by pipeline applicability, business-step signatures, branch routing, terminal merge, replayed pipeline values, and business-facing diagnostics.
- **Representation identity** (`transportInputType` and `transportOutputType`) is the transport serialization type used for protobuf parser selection, Kafka/SQS/webhook payload conversion, await envelopes, and transport-facing diagnostics.

Version 2 did not need this distinction because its pipeline contract and transport representation normally used the same application type. Version 3 does: generated records, wrappers, and unions are canonical domain contracts, while generated protobuf messages are the representation that crosses a gRPC or await transport boundary. Reusing one descriptor field for both meanings caused protobuf union wrappers to escape into branch execution and made durable completion validation ambiguous.

Await is the hardest boundary because it persists work across a dispatch, an external interaction, a completion, and a later resume or replay. New interaction records therefore persist the canonical `outputType` and its `transportOutputType`. Input identities are reconstructed from the stable `stepId` by rebuilding the `AwaitStepDescriptor`; legacy records without a transport output type default it to the stored canonical output type. A rebuilt descriptor whose canonical output type differs from the durable record is a release-compatibility failure, not a value to guess.

The rule is intentional: protobuf may appear in transport metadata and immediately around serialization/deserialization, but it must not become a resumed pipeline value or a v3 business-step value. Generated adapters convert canonical-to-protobuf immediately before dispatch and protobuf-to-canonical immediately after transport decoding. This keeps the wire contract durable without making protobuf part of canonical business execution.

## Current representation consumers

| Boundary | Canonical value | External representation / mechanism | Build-time owner | Runtime owner | Current disposition |
| --- | --- | --- | --- | --- | --- |
| Protobuf/gRPC | Generated records, wrappers, unions | Generated protobuf adapters | V3 contract generation | Generated adapters | Generated; retain outside generic mappings |
| REST/JSON | Existing application or transport values | Jackson and protobuf JSON paths | REST renderers | Runtime JSON codecs | Experimental v3 policy; do not infer from `mappings` |
| Await | Generated v3 payload | Canonical-to-protobuf dispatch and protobuf-to-canonical resume | Contract generation | Await transports | Supported generated transport path; durable descriptor stores both identities |
| Checkpoint | Current checkpoint payload | JSON envelope / payload reference | Checkpoint renderers | Checkpoint runtime | Not a v3 representation consumer yet |
| Persistence | Generated record | Explicit representation plus exact mapper | Side-effect generation | Persistence plugin | Supported record-write boundary |
| OpenCSV | Generated `PaymentRecord` | Provider-generated facade plus explicit CSV row and generic mapper | Representation provider | Generated facade | Supported provider reference implementation; core has no OpenCSV-specific semantics |
| Object publish | Terminal business value | Existing output mappers | Boundary generation | Publish target | Application-owned conversion remains |
| Kafka | Await/connector payload | Existing transport envelope and protobuf JSON | Transport generation | Kafka adapters | Do not duplicate protobuf adapters |
| Command/effect connectors | Future canonical contracts | No v3-specific representation path | N/A | N/A | Deferred |

The present duplication is deliberate where protobuf adapters already preserve canonical semantics. The OpenCSV provider owns facade generation and mapper injection; the reader remains responsible only for parsing external rows and establishing deterministic raw-row identity.

## Next vertical slice

The next material improvement is direct JSON for generated v3 records and wrappers at a tightly bounded REST or connector boundary. Keep generated domain classes framework-agnostic; use framework-owned Jackson configuration and reject unions or unsupported shapes explicitly. Do not introduce a `direct`, `generated`, or `custom` DSL mode, and do not create another canonical union serialization protocol while protobuf adapters own transport unions.

## Roadmap drafts

### Horizon 1 — releasable v3

1. **Publish v3 preview readiness and diagnostics.** Deliver the support matrix, clean-regeneration proof, deterministic mapping diagnostics, and canonical-versus-external documentation. Exclude new provider capabilities and new targets. This blocks preview only when Gate A evidence fails.
2. **Harden representation-mapping conformance.** Keep the normalized model generic across every named type kind; preserve persistence record-only capability diagnostics. Exclude generated entities and component-specific mapper SPIs. This blocks preview only for nondeterministic lookup or diagnostics.
3. **Keep CSV Payments as the executable v3 proof.** Completed for the current v3 persistence profile: clean generated-source regeneration, the OpenCSV mapper boundary, persistence identity separation, protobuf await, canonical branches, and terminal merge are exercised without topology redesign. Keep the dedicated CI lane required for regression coverage.

### Horizon 2 — ergonomic built-in representations

1. **Direct JSON for records and wrappers.** Add the bounded JSON vertical above; exclude unions and generated JSON DTOs.
2. **Persistence read/query maturity.** Add pair-accurate reverse mapping and explicit identity semantics; exclude generated JPA entities.
3. **Boundary convergence.** Use generic mappings only where they remove real duplicated resolution; retain generated protobuf adapters.
4. **App-generator ownership reduction.** In a separate clean checkout, classify and retire only deterministic DTO/protobuf mapper output now replaced by v3 generation. Preserve user-owned scaffolds and custom mapper examples.

### Horizon 3 — representation provider platform

#### Active compiler-host bridge

The public Representation Provider SPI is host-neutral: providers receive normalized canonical types, mapping requests,
boundary claims, resolved representations, schema fragments, and artifact descriptions. They do not receive JSR-269,
Quarkus, Maven, renderer, or filesystem-writer types.

TPF's current production build host is nevertheless the JSR-269 processor. That host owns source-symbol discovery,
normalizes the YAML and Java source model into provider requests, discovers provider JARs through `META-INF/services`
from its annotation-processor classloader, resolves ordered claims, and is the only process that writes generated
sources or resources. Maven application dependencies make a provider available at runtime; annotation-processor-path
dependencies make it available during generation. Those are deliberately separate concerns.

OpenCSV is the reference boundary: its provider recognizes the provider marker, validates the explicit row type and
`Mapper<Canonical, External>` pair, describes a canonical blocking-iterator facade, and owns the injected mapper and
`fromExternal` conversion. Core never contains an OpenCSV key, row type, convention, or renderer. The authored CSV
reader therefore remains an external-row reader with stable raw-row identity only; canonical values begin in the
provider-generated facade.

This bridge is not a new permanent compiler host. A future build host can supply the same neutral requests and consume
the same artifact descriptions, while retaining the invariant that providers describe artifacts and the host writes
them.

1. **Completed reference implementation.** The host-neutral SPI now proves dependency discovery, normalized-model access, mapping resolution, host-owned artifact writing, schema composition, and an OpenCSV consumer—without migrating protobuf.
2. **Provider lifecycle migration.** Gradually move additional built-in boundaries only where provider ownership removes real duplication; preserve generated protobuf transport adapters.
3. **Third-party and target expansion.** Extend provider-owned configuration/documentation proof and target-specific capability declarations without promising all-language parity.

## Existing issue disposition

- **#510:** leave closed. It remains the completed v3 language-model roadmap. A future linking comment should identify CSV Payments and #531 as the executable proof and point provider work to new Horizon 3 issues.
- **#509:** keep open, narrowed to remaining planner/MCP metadata and executable-documentation work. Its acceptance should reference the real CSV Payments proof rather than the obsolete synthetic example, and it is not a preview-release blocker.

## Verification

- Run focused loader, generator, IDL, branch, persistence, and CSV mapping tests.
- Run framework verification with `-Dmaven.repo.local="$PWD/.m2/repository"`.
- Run the CSV pipeline-runtime proof with the v3 persistence profile and the targeted E2E path. The `CSV Pipeline Runtime v3 Persistence E2E` reusable CI variant rebuilds the images from clean module output and uploads generated-source diagnostics separately on failure.
- Build the docs site after navigation and support-status updates.

### Current worktree result

The focused loader, generator, persistence, OpenCSV, await, and CSV representation tests pass. The clean pipeline-runtime build regenerates compatible v3 protobuf and Java sources when `-Dcsv.v3.persistence=true` selects the v3 template. The targeted `CsvPaymentsPipelineRuntimeEndToEndIT` then completes successfully against those images: five records are published and persisted, including approved and unapproved await branches. Generated protobuf values are confined to the gRPC/await transport boundary; canonical payloads reach business steps, branch routing, and terminal merge. This completes the CSV Payments Gate A proof obligation.
