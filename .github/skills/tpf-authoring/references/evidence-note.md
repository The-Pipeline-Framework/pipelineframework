# TPF Author Skill evidence note

This note records the evidence and archaeology behind the Skill. It is intentionally
not a second authoring guide. The detailed pre-edit matrix is in `coverage-audit.md`.

## Bases and repository evidence

The audit compared PR #690 branch `tpf-skill` at `f817e1a9` with current
`origin/main` at `4c7e59b1` on 2026-08-23.

High-value current sources included:

- architecture/state: `docs/design/fcis.md`, `application-structure.md`,
  `state-model.md`, execution safety, Await, persistence/cache, and object I/O docs;
- types/steps/operators: `docs/develop/pipeline-template-dsl.md`, `code-a-step.md`,
  `pipeline-step.md`, operator/delegation/service-contract docs, and compiler/runtime
  tests for cardinality, generated domain types, union applicability, nested pipelines,
  direct recursion, blocking bridges, mapper selection, and operator resolution;
- representations: canonical-domain readiness, `PipelineTemplateTypeMappings`, Java
  domain binding and mapper inference, representation-provider API/generation phases,
  file/OpenCSV providers, persistence boundaries, `PayloadReference`, materializers,
  durable codecs, and object ingest/publish tests;
- execution: Query/Command/Await support and stores, connector-provider manifests and
  operations, aspects, checkpoint handoff, retry/DLQ/circuit docs, and focused replay,
  identity, duplicate, recovery, and target-safe cache tests;
- deployment/build: runtime-layout and POM lifecycle docs, mapping loader/resolver,
  generation phases and `META-INF/pipeline/` tests, plus current CSV Payments, Search,
  Checkout, Restaurant Approval, stdio/object, Spring, Query, Command, and Await fixtures;
- configuration: `docs/develop/configuration/all-settings.md`, then the matching
  processor/config loaders, telemetry capability/policy/resource code, provider loaders,
  generated client wiring, execution defaults, and circuit-boundary resolution tests;
- history: recent canonical type, repeated-field, payload representation, object I/O,
  Await, Query/provider, aspect applicability, cache replay, and packaging changes.

Examples were treated as executable compatibility evidence, not universal scaffolds.
CSV Payments in particular contains current native paths plus historical build/application
residue.

## Audit outcome

The important omissions were the service/operator/cardinality decision, reactive versus
blocking/backpressure trade-offs, canonical/Java/persistence/wire identity separation,
representation-consumer selection, grouped object ingest, connector configuration
lifetimes, resilience and checkpoint ownership, generated build artifacts, bootstrap/
testing search rules, and the current simple-first deployment prior.

`SKILL.md` now carries only the always-needed additions: the four cardinality choices,
ordinary-service-versus-operator rule, the four representation identities, the two
different meanings of mapping, simple-first deployment, semantic-owner-first framework
change process, configuration-lifetime/telemetry routing, and routes to deeper guidance.
Its line count decreased from 164 to 155.

The former `composition-and-placement.md`, `payload-representations.md`,
`external-interactions.md`, and `state-and-replay.md` were replaced—not retained beside
the new structure—by four on-demand references:

- `authoring-model.md`: canonical modeling, services/operators, cardinality,
  backpressure, composition/recursion, failure channels, and testing;
- `representations-and-runtime-mappings.md`: Java bindings, consumer mappings,
  providers, payload/object boundaries, and durable/wire identity;
- `execution-and-replay.md`: Query/Command/Await, connector lifetimes, distinct state
  authorities, resilience/recovery, and checkpoint handoff;
- `deployment-and-packaging.md`: logical placement versus physical artifacts,
  configuration lifetimes and telemetry, simple/split deployment decisions, generated
  build boundaries, bootstrap, and tests.

## What `all-settings.md` added

The settings page is valuable because it exposes configuration as several lifetimes,
not one bag of properties. The audit retained these stable decisions:

- `pipeline.yaml` owns portable semantic choices and generated contract intent;
- build-time/augmentation inputs determine which adapters, metadata, and telemetry
  capabilities exist, so some changes require a rebuild;
- runtime framework policy owns concurrency, backpressure, retry/circuit behavior,
  durability/provider choice, background execution, Await plumbing, and health;
- deployment wiring owns concrete endpoints, brokers, storage, secrets, exporters, and
  telemetry backends;
- dynamic business facts remain typed pipeline input;
- global defaults are preferred, with exact step/boundary overrides reserved for real
  operational exceptions and keyed from generated diagnostics.

Telemetry was the most important omission: generated item-boundary metadata and artifact
capabilities are separate from runtime policy, which is separate again from exporter/
backend configuration. The Skill now routes configuration and telemetry work to the
deployment reference and warns that a runtime toggle cannot add build-absent signals.

The exact Kafka, SQS, S3, PostgreSQL, Redis, provider-class, port, timeout, and property
catalogue was deliberately not copied. The current settings page is the search map for
those details. Its compatibility/annotation/template-generator entries and composed
configuration prefixes still need verification against the current loader/processor and
chosen runtime before use.

## What the retired app-generator taught us

The old `/Users/mari/IdeaProjects/app-generator` was inspected as archaeology. Its
generator normalized YAML, planned scaffolds, selected dependencies, emitted Maven
modules/configuration/bootstrap, generated DTOs/mappers/connectors, and created topology
and test assets. The significant behavior classifies as follows:

| Class | Generator knowledge | Disposition |
| --- | --- | --- |
| A — still required author knowledge | validated `pipeline.yaml`; canonical contract and step/cardinality choices; processor/runtime/provider dependencies visible to the compiling module; runtime config; genuine mappers/provider code; build and test the topology actually deployed | Taught through core decisions and deployment/representation references, without a frozen POM template |
| B — now compiler/generated | canonical Java/protobuf domain artifacts; union/routing and transport adapters; generated role sources and pipeline metadata; provider manifests/descriptors/registration; supported aspect and I/O facades | Skill tells agents to compile/inspect generation before writing equivalents |
| C — simplified by current TPF | v3 canonical types replace much DTO/protobuf/union boilerplate; representation providers own file/OpenCSV facades; named connector bindings separate provider/operation/input; monolith layout can yield one runnable unit when build topology matches | Encoded as search/decision rules, not support promises |
| D — obsolete assumption | every step needs its own Maven/runtime module; dedicated orchestrator/persistence/cache modules are always required; runtime layout rewrites POMs; generated host/factory/stub classes belong to the app; positional generator API; handwritten canonical DTO/proto/union mirrors; old scaffold layouts are doctrine | Explicit warnings in deployment reference; do not revive |
| E — application-specific | chosen ports, certificates, Postgres/Kafka defaults, Docker scripts/images, package names, sample provider endpoints, example module names, and test-container matrix | Omitted from the Skill |

The stable lesson is not “reproduce the generator.” It is: ensure the chosen build
exposes the compiler/runtime/provider inputs it needs, then let current generation own
the deterministic artifacts.

## Repowise findings retained as leads

`repowise decision list --status all --format json` returned 34 decisions; all were
`proposed`. None became doctrine by status alone.

- `7e4bb654`, `5a4d88c2`, `460dd001`, `4c176833`: compiler-owned canonical DSL/IDL
  state and exclusion of wire metadata. Current template DSL, loaders, generators, IDL
  tests, and generated adapters verify the broader rule; exact tag mechanics stay out.
- `11ebf7f6`: CSV Payments object-I/O migration. Current object ingest/publish,
  `PayloadReference`, representation provider, and example tests verify the ownership
  transfer and deletion heuristic.
- `817224fe`: keep LLM adapter tuning outside portable bindings. Current connector docs
  and implementation verify the broader provider-config/operation-config/input split.
- `7a8af571`, `0a47ca5c`, `cbd4ce89`: live/durable Await boundaries. Current Await
  descriptors, admission, completion, continuation, and recovery tests verify only the
  transport-neutral suspension rules retained by the Skill.

Repowise did not contain a decision for runtime layout versus Maven build topology.
That doctrine came from current deployment docs, mapping code/tests, and examples.

## Repowise findings rejected

- Renderer extraction, queue-async class slicing, read-model/operations splits, and
  other PR-level refactor plans are internal implementation choices.
- CI cache, Testcontainers, smoke-template placement, and virtual-thread smoke details
  are transient build/operations concerns.
- Low-confidence inferred/example decisions such as a CSV sealed superclass or exact
  replay prerequisites are not application-authoring doctrine.
- Exact provider catalogues, support matrices, retry counts, store schemas, and current
  renderer/module names are release details.
- Proposed immutable control-plane patterns were not generalized to every existing
  store; current execution/Await storage includes legacy exceptions.
- Hotspot/dependent counts were used to set review rigor only. They were rejected as a
  reason to place semantics in a lower-impact sibling abstraction or additive sidecar.

## Current documentation gaps / unresolved questions

- The runtime-layout docs still describe “after scaffold generation,” while the retired
  generator is no longer the canonical onboarding mechanism. There is no single current,
  minimal bootstrap/package page that replaces its useful dependency/classpath setup.
- One runnable monolith is proven, but current docs expose example-specific
  `monolith-svc` source aggregation and alternate POM mechanics. The stable simple-first
  prior is clear; exact “single deployment unit JAR” ergonomics are not cleanly
  documented enough to encode beyond current build topology evidence.
- Representation mappings are generic in the model but consumer support remains uneven.
  Current readiness docs, compiler diagnostics, and consumer tests must be checked.
- Operator pass-through/union applicability is clearer in compiler/applicability tests
  than in one author-facing decision guide. The Skill retains only type-based routing.
- Aspect observer applicability across union branches is implemented/tested but weakly
  surfaced in author docs.
- Local/remote/operator/cardinality/function and Spring parity remain release-specific;
  compile and test the intended runtime rather than relying on a memorized matrix.
- Retry/redrive availability across Command providers and layouts continues to evolve.

## Lightweight validation

- `git diff --check`, relative Markdown-link checks, YAML parsing, one-entry-point
  discovery, stale-reference checks, and an explicit architectural-coverage assertion
  passed.
- The skill-creator `quick_validate.py` entrypoint could not import its undeclared
  `PyYAML` dependency in the available Python runtimes. The same frontmatter key/name/
  description/TODO checks were run with the available Ruby YAML parser and passed.
- No Maven verification or CodeRabbit review was run, as requested.
