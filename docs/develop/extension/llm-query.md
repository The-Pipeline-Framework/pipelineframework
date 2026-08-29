# One-turn LLM Query

The LLM Query connector asks a model to make exactly one typed application decision. It uses the ordinary provider-backed `kind: query` path, so pipeline caching and Query capture can replay the returned decision without another inference.

The model never executes a connector operation. A call is inert proposal data. Authorization and dispatch belong to a later TPF step boundary.

```text
application state + release-pinned callable catalogue
                         ↓
                    LLM Query
                         ↓
       call:<tpf.llm.AgentCall> | complete:ApplicationResult
```

## Define the decision

Reference the connector-owned protocol type from an application-authored union:

```yaml
version: 3
appName: invoice-agent
basePackage: example.invoice
transport: LOCAL

types:
  InvoiceState:
    fields:
      - [invoiceId, string]
      - [amount, decimal]

  ChargeArguments:
    fields:
      - [invoiceId, string]
      - [amount, decimal]

  InvoiceResult:
    fields:
      - [status, string]

  InvoiceDecision:
    variants:
      call: <tpf.llm.AgentCall>
      complete: InvoiceResult
```

`AgentCall` contains only `binding`, `operation`, and canonical `argumentsJson`. It has no provider identity, credentials, runtime handle, hidden reasoning, execution ID, or authority to invoke the selected operation.

For a one-turn application completion with no tool selection, use an ordinary non-union output and
omit `callables`. TPF exposes one required `complete` alternative whose schema is that authored output
type. Configuring `callables` for this mode is rejected.

When trusted input context must accompany a smaller model-authored value, the output may instead be a
record envelope and `config.completion` may name the model-authored field plus input paths to carry:

```yaml
    output: ReviewReady
    config:
      modelInputExcludes:
        documentId: documentId
        invoice: invoice
      completion:
        field: review
        documentId: documentId
        invoice: invoice
      instructions: Analyse the evidence and return InvoiceReview.
```

Here the model schema is only `ReviewReady.review`; `documentId` and `invoice` are copied from the
typed Query input after schema validation. `modelInputExcludes` also keeps those paths out of the
model-state JSON and prevents excluded `payload_ref` values from being materialized as media. Every
other envelope field must have an explicit mapping, and dotted paths address nested record fields.
This projection does not perform another inference or allow model output to overwrite trusted
application context.

## Configure the Query

The named LLM binding owns model configuration. The Query step owns its instructions and explicitly exposed callable catalogue:

```yaml
connectors:
  model:
    provider: llm.query
    version: 1
    config:
      model: qwen3:8b
      baseUrl: http://localhost:11434

  payments:
    provider: acme.payments
    version: 1
    config:
      connection: payments-production

steps:
  - name: Decide invoice
    kind: query
    cardinality: ONE_TO_ONE
    input: InvoiceState
    output: InvoiceDecision
    using: model
    operation: decide
    operationVersion: 1
    config:
      instructions: Decide whether to propose the charge or complete the invoice.
      structuredOutputSchema: REQUIRED
    callables:
      charge:
        using: payments
        operation: charge.create
        operationVersion: 1
        kind: command
        input: ChargeArguments
        commandIdGenerator: example.invoice.ChargeCommandIdGenerator
```

`using` and `operation` select the configured capability. The compiler verifies `kind`, `operationVersion`, and `input` against the selected operation's normalized type contract from Connector metadata; they are never trusted model output. Command callables that can be invoked also declare their ordinary command ID generator and may declare the existing duplicate and command policies.

The compiler emits the canonical v3 catalogue into the release contract. At runtime the connector projects the selected input types and only their transitively reachable definitions into model-safe JSON Schema, then validates returned arguments against the same canonical metadata. Unrelated pipeline types are not exposed to the model. The schema is a projection for the model, not an alternative application schema language. Unknown aliases, missing or extra fields, malformed JSON, and type mismatches become `TerminalFailure("invalid-model-decision")`.

Model aliases are untrusted observations. TPF looks up the exact compiled alias and constructs `binding + operation` from the catalogue; it never copies a provider or operation identity supplied inside model arguments.

`structuredOutputSchema` defaults to `REQUIRED`. In required mode an adapter that cannot enforce the supplied decision schemas for the compiled alternatives fails before inference. The LangChain4j adapter uses the selected provider's native JSON Schema response format for a single direct `complete` alternative; multi-alternative tool selection is not claimed as natively schema-enforced. `OPTIONAL` is an explicit best-effort mode for prompt-guided tool selection; TPF still validates the single response against the canonical v3 contract. Neither mode performs a hidden repair call or reinference.

## Invoke one proposal

A following ordinary step dynamically invokes exactly one capability from the named Query step's compiled catalogue:

```yaml
  - name: Invoke proposal
    input: <tpf.llm.AgentCall>
    output: <tpf.connector.OperationObservation>
    operation:
      mode: dynamic
      from: Decide invoice
```

The generated invocation adapter revalidates the proposed `binding + operation` and canonical arguments before invoking a provider. Query operations use captured Query semantics; Command operations use the existing effect store, idempotency, duplicate, confirmation, ambiguity, and user-action semantics. Dynamic operation selection is binding mechanics, not a new semantic step kind; it neither loops nor updates application state.

`OperationObservation` is a discriminated union:

- `result` carries the bound operation identity, normalized outcome/code, canonical result type, and canonical `resultJson`;
- `empty` carries the same identity and outcome/code without inventing a result payload.

`QueryOutcome.NotFound` becomes an `empty` observation with outcome `not-found`, because absence is normally information the next application decision may need. `Found` and successful Commands become `result` observations. `TemporarilyUnavailable`, authentication/authority failures, terminal failures, Command ambiguity, confirmation barriers, and user-action requirements retain their existing Query/Command failure or effect-state semantics; they are not flattened into successful observations.

The application maps the generic observation into its own state in a later Mapper, service, or reducer step. Dynamic invocation never manufactures `InvoiceState`, `EngineeringState`, or another application-owned type.

## Compose multiple turns with ordinary recursion

Agentic behavior does not require a second Agent runtime. Put the one-turn Query, dynamic native
operation, and application-authored reducer inside a named v3 pipeline, then route the reducer's
state back through an ordinary bounded self-invocation:

```text
State -> LLM Query -> call:<tpf.llm.AgentCall>
      -> operation.mode: dynamic -> native Query | Command
      -> <tpf.connector.OperationObservation> -> authored reducer
      -> State -> pipeline:self

State -> LLM Query -> complete:ApplicationResult -> terminal
```

The model still makes exactly one decision per Query invocation. The dynamic adapter invokes at
most one release-pinned capability and never chooses whether another turn is needed. The reducer
alone interprets the observation and advances application state; the existing recursive pipeline
depth limit provides the framework-owned safety bound.

The offline [Agent Composition Proof](https://github.com/The-Pipeline-Framework/pipelineframework/tree/main/examples/agent-composition-proof) exercises
the complete path with a Query `NotFound` observation, a durable Command, typed completion, generated
metadata, and a stateless adapter whose response depends only on canonical `AgentState.phase`.

After compilation, inspect `META-INF/pipeline/pipeline-contract.json` for contributed protocol types,
the dynamic operation descriptor, and the named recursive binding; `order.json` for the finite root
invocation; and `branching.json` for the `call`/`complete` routes. The existing
`connector-bindings.json` exposes the release-pinned callable catalogue as generated inspection
metadata. Runtime capabilities are resolved from `PipelineYamlConfig` and the trusted
`ConnectorProviderManifestCatalog`; none of these artifacts introduces an Agent runtime or semantic
dispatch step kind.

This composition follows the existing decisions for [bounded recursion](../../decisions/0004-nested-composition-and-bounded-recursion.md),
[Query observation](../../decisions/0005-query-is-external-observation.md),
[Command effects](../../decisions/0006-command-owns-logical-effects.md), and
[generated connector boundaries](../../decisions/0010-generated-boundaries-and-connectors.md).

## Adapter boundary

Add the LangChain4j adapter:

```xml
<dependency>
  <groupId>org.pipelineframework</groupId>
  <artifactId>llm-query-langchain4j-connector</artifactId>
  <version>${pipelineframework.version}</version>
</dependency>
```

The original `llm.query` provider uses Ollama:

```yaml
connectors:
  local-model:
    provider: llm.query
    version: 1
    config:
      model: qwen3:8b
      baseUrl: http://localhost:11434
```

Ollama runtime tuning is configured through ordinary runtime properties rather
than the portable connector binding:

```properties
pipeline.llm.langchain4j.ollama.request-timeout=PT30S
pipeline.llm.langchain4j.ollama.thinking=true
```

The timeout must be a positive ISO-8601 duration. These properties retain the
adapter's prior defaults when omitted. The adapter disables LangChain4j's
internal retries so one TPF Query execution cannot silently become multiple
model inferences; retry policy remains owned by the pipeline execution.

OpenAI-compatible chat-completion providers use the same portable Query
contract and media mapping:

```yaml
connectors:
  hosted-model:
    provider: llm.query.openai.compatible
    version: 1
    config:
      model: google/gemini-3.1-flash-lite
      baseUrl: https://openrouter.ai/api/v1
```

The API key is a runtime secret and must not be placed in the connector binding:

```properties
pipeline.llm.langchain4j.openai-compatible.api-key=${OPENROUTER_API_KEY}
pipeline.llm.langchain4j.openai-compatible.request-timeout=PT60S
```

The key is required only when an `llm.query.openai.compatible` binding starts.
Missing credentials and non-positive timeouts fail before inference.
Applications should pin a model whose provider advertises native JSON Schema
support when `structuredOutputSchema` remains `REQUIRED`.

The adapter uses LangChain4j's low-level chat/tool-proposal API. It performs one chat call, returns one proposed alias plus arguments, and never installs a tool executor or autonomous Agent loop.

Operational model/provider failures remain exceptional Query failures. A syntactically or structurally invalid model decision is instead a typed terminal Query outcome. Query capture records the successful application decision, not prompts, credentials, SDK objects, or hidden reasoning. The durable DynamoDB store persists only a SHA-256 input fingerprint and the canonical typed decision; its 300 KiB event limit requires `PayloadReference` for larger values.

## Deliberate limits

- No recursive tool loop, Agent runtime, Agent state, memory bag, or Agent execution identity exists.
- No runtime discovery snapshot or dynamic grant subsystem is required for the release-pinned v1 catalogue.
- No MCP or AskUser protocol is included.
- `argumentsJson` is canonical serialized data for the selected operation input contract, not a general JSON escape hatch.
