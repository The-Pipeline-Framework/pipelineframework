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
    callables:
      charge:
        using: payments
        operation: charge.create
        operationVersion: 1
        kind: command
        input: ChargeArguments
```

`using`, `operation`, `kind`, `operationVersion`, and `input` are build-time declarations, not model-authored fields. The current Connector manifest identifies operations by provider, operation, kind, and major version and does not publish operation input contracts. TPF therefore validates these declarations against the selected binding's release metadata and the normalized v3 type graph instead of pretending `using + operation` alone is sufficient.

The compiler emits the canonical v3 catalogue into the release contract. At runtime the connector projects the selected input types into model-safe JSON Schema and validates returned arguments against the same canonical metadata. The schema is a projection for the model, not an alternative application schema language. Unknown aliases, missing or extra fields, malformed JSON, and type mismatches become `TerminalFailure("invalid-model-decision")`.

Model aliases are untrusted observations. TPF looks up the exact compiled alias and constructs `binding + operation` from the catalogue; it never copies a provider or operation identity supplied inside model arguments.

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

- No operation dispatch or authorization is implemented here.
- No recursive tool loop, Agent runtime, Agent state, memory bag, or Agent execution identity exists.
- No runtime discovery snapshot or dynamic grant subsystem is required for the release-pinned v1 catalogue.
- No MCP or AskUser protocol is included.
- `argumentsJson` is canonical serialized data for the selected operation input contract, not a general JSON escape hatch.
