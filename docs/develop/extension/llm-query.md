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

The compiler emits the canonical v3 catalogue into the release contract. At runtime the connector projects the selected input types into model-safe JSON Schema and validates returned arguments against the same canonical metadata. The schema is a projection for the model, not an alternative application schema language. Unknown aliases, missing or extra fields, malformed JSON, and type mismatches become `TerminalFailure("invalid-model-decision")`.

Model aliases are untrusted observations. TPF looks up the exact compiled alias and constructs `binding + operation` from the catalogue; it never copies a provider or operation identity supplied inside model arguments.

`structuredOutputSchema` defaults to `REQUIRED`. In required mode an adapter that cannot enforce the supplied decision schemas fails before inference. `OPTIONAL` is an explicit best-effort mode for prompt-guided JSON; TPF still validates the single response against the canonical v3 contract. Neither mode performs a hidden repair call or reinference.

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

## Adapter boundary

Add the LangChain4j adapter for Ollama:

```xml
<dependency>
  <groupId>org.pipelineframework</groupId>
  <artifactId>llm-query-langchain4j-connector</artifactId>
  <version>${pipelineframework.version}</version>
</dependency>
```

The adapter uses LangChain4j's low-level chat/tool-proposal API. It performs one chat call, returns one proposed alias plus arguments, and never installs a tool executor or autonomous Agent loop.

Operational model/provider failures remain exceptional Query failures. A syntactically or structurally invalid model decision is instead a typed terminal Query outcome. Query capture records the successful application decision, not prompts, credentials, SDK objects, or hidden reasoning.

## Deliberate limits

- No recursive tool loop, Agent runtime, Agent state, memory bag, or Agent execution identity exists.
- No runtime discovery snapshot or dynamic grant subsystem is required for the release-pinned v1 catalogue.
- No MCP or AskUser protocol is included.
- `argumentsJson` is canonical serialized data for the selected operation input contract, not a general JSON escape hatch.
