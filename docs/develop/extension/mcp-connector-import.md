# Import MCP tools as Connector operations

TPF can import selected Model Context Protocol (MCP) tools as ordinary, release-pinned Connector
operations. Use this when an external system already exposes a useful MCP server and the capability
belongs at a typed Query or Command boundary in your pipeline.

The model-facing protocol does not change. An LLM proposes a `binding + operation + arguments`
call, and TPF invokes it through the same dynamic operation path whether its implementation is
native Java or MCP-backed.

This guide uses QuickBooks Online as a concrete example. The same workflow applies to other MCP
servers.

::: tip Version
MCP Connector import is available from `26.9.2-SNAPSHOT`. Keep the importer plugin, runtime
connector, and application on the same exact TPF version.
:::

## Understand the three gates

An MCP tool passes through three deliberately separate states:

1. **Discovered**: the configured MCP server advertised the tool during an explicit refresh.
2. **Imported**: an author mapped that exact MCP name to a TPF operation identity, Query or Command
   semantics, a major version, and canonical input/output type names.
3. **Callable**: a particular LLM Query step exposes the imported operation in its release-pinned
   `callables` catalogue.

```text
MCP discovery
    ↓ explicit author selection
pinned TPF operation import
    ↓ named Connector binding
release-pinned callable exposure
    ↓ ordinary AgentCall dispatch
Query or Command semantics
```

Discovery never grants authority. Import does not make an operation callable.

## Start with one read-only operation

Begin with a read-only tool against a QuickBooks sandbox. For example, map a customer-search tool
to a TPF Query:

```text
QuickBooks customer search
        ↓
TPF Query: quickbooks.customer.search
        ↓
QuickBooksCustomerSearchRequest → QuickBooksCustomerSearchResult
```

This lets you prove authentication, schema import, Query capture/replay, and canonical result
validation before introducing financial effects.

Intuit offers two relevant server shapes:

- The hosted Intuit MCP service uses Streamable HTTP. At the time of writing it is an
  invitation-only pilot and advertises tools such as `qbo_contact_search_customer`.
- The open-source Intuit QuickBooks Online MCP server runs locally over STDIO and advertises tools
  such as `search_customers`.

The exact advertised names and schemas belong to the selected server version. Pin the server
version and review every refresh diff.

## Add the importer and runtime connector

Add the runtime connector as an application dependency:

```xml
<dependency>
  <groupId>org.pipelineframework</groupId>
  <artifactId>mcp-connector</artifactId>
  <version>${pipelineframework.version}</version>
</dependency>
```

Add the importer plugin, but do not bind `refresh-import` to the normal Maven lifecycle. Import is
an explicit contract-review action, not something every production build should rediscover.

### Streamable HTTP example

This example selects one customer-search tool from the hosted QuickBooks server:

```xml
<plugin>
  <groupId>org.pipelineframework</groupId>
  <artifactId>connector-mcp-maven-plugin</artifactId>
  <version>${pipelineframework.version}</version>
  <configuration>
    <transport>streamable-http</transport>
    <endpoint>https://mcp.quickbooks.intuit.com/mcp</endpoint>
    <headers>
      <!-- Values name host environment variables; they are not credentials. -->
      <Authorization>QBO_MCP_AUTHORIZATION</Authorization>
      <User-Agent>QBO_MCP_USER_AGENT</User-Agent>
    </headers>
    <tools>
      <tool>
        <mcpName>qbo_contact_search_customer</mcpName>
        <operation>quickbooks.customer.search</operation>
        <kind>query</kind>
        <majorVersion>1</majorVersion>
        <inputType>QuickBooksCustomerSearchRequest</inputType>
        <outputType>QuickBooksCustomerSearchResult</outputType>
      </tool>
    </tools>
  </configuration>
</plugin>
```

Provide refresh credentials through the host environment:

```bash
export QBO_MCP_AUTHORIZATION="Bearer ..."
export QBO_MCP_USER_AGENT="partner_app_MyPipeline"
```

Header values are read only while refreshing and are never written into an imported artifact.

### STDIO example

For a locally installed server, configure the executable and arguments instead:

```xml
<plugin>
  <groupId>org.pipelineframework</groupId>
  <artifactId>connector-mcp-maven-plugin</artifactId>
  <version>${pipelineframework.version}</version>
  <configuration>
    <transport>stdio</transport>
    <command>node</command>
    <arguments>
      <argument>/opt/quickbooks-mcp/dist/index.js</argument>
    </arguments>
    <environment>
      <!-- Child-process key → host environment-variable name. -->
      <QUICKBOOKS_CLIENT_ID>QBO_CLIENT_ID</QUICKBOOKS_CLIENT_ID>
      <QUICKBOOKS_CLIENT_SECRET>QBO_CLIENT_SECRET</QUICKBOOKS_CLIENT_SECRET>
      <QUICKBOOKS_REFRESH_TOKEN>QBO_REFRESH_TOKEN</QUICKBOOKS_REFRESH_TOKEN>
      <QUICKBOOKS_REALM_ID>QBO_REALM_ID</QUICKBOOKS_REALM_ID>
      <QUICKBOOKS_ENVIRONMENT>QBO_ENVIRONMENT</QUICKBOOKS_ENVIRONMENT>
    </environment>
    <tools>
      <tool>
        <mcpName>search_customers</mcpName>
        <operation>quickbooks.customer.search</operation>
        <kind>query</kind>
        <majorVersion>1</majorVersion>
        <inputType>QuickBooksCustomerSearchRequest</inputType>
        <outputType>QuickBooksCustomerSearchResult</outputType>
      </tool>
    </tools>
  </configuration>
</plugin>
```

During refresh, the importer is the temporary STDIO process host and closes the process with its
client. That does not change runtime ownership: the application host owns runtime process creation,
supervision, restart, and shutdown.

## Refresh and review the pinned contract

Run the import explicitly:

```bash
./mvnw connector-mcp:refresh-import \
  -Dmaven.repo.local="$PWD/.m2/repository"
```

The importer connects, calls MCP discovery, selects only the configured tool names, normalizes
their schemas into canonical TPF metadata, and writes:

```text
src/main/resources/META-INF/pipeline/connector-providers.json
src/main/resources/META-INF/pipeline/mcp-tools.json
```

Commit and review both resources. Ordinary application builds consume them offline and do not need
the MCP server merely to reconstruct the release contract.

### Standard Connector metadata

`connector-providers.json` contains the imported operation and canonical protocol types using the
same public metadata vocabulary as a native Connector. A simplified customer-search import might
look like this:

```json
{
  "schemaVersion": 6,
  "providers": [
    {
      "id": "mcp.client",
      "version": { "major": 1, "minor": 0 },
      "configurationSchema": {
        "id": "mcp.client.provider",
        "version": 1,
        "fields": [
          { "name": "connection", "type": "CONNECTION_REF", "required": true }
        ]
      },
      "operations": [
        {
          "id": "quickbooks.customer.search",
          "kind": "tpf:query",
          "majorVersion": 1,
          "queryCapabilities": { "cacheability": "LIVE_ONLY" },
          "queryCardinality": "ONE_TO_ONE",
          "typeContract": {
            "input": "QuickBooksCustomerSearchRequest",
            "output": "QuickBooksCustomerSearchResult"
          }
        }
      ],
      "protocolTypes": [
        {
          "name": "QuickBooksCustomerSearchRequest",
          "fields": [
            { "name": "searchTerm", "type": "string" }
          ]
        },
        {
          "name": "QuickBooksCustomerSummary",
          "fields": [
            { "name": "id", "type": "string" },
            { "name": "displayName", "type": "string" }
          ]
        },
        {
          "name": "QuickBooksCustomerSearchResult",
          "fields": [
            {
              "name": "customers",
              "type": "<mcp.client.QuickBooksCustomerSummary>",
              "repeated": true
            }
          ]
        }
      ]
    }
  ]
}
```

The actual generated fields come from the discovered server schema; do not hand-maintain this
example as a substitute for refresh.

### Private MCP execution pin

`mcp-tools.json` version 2 preserves the external tool name, original input/output schemas,
their hashes, the importer projection identity, and the selected result mode. These private
adapter contracts stay outside model-visible callable metadata:

```json
{
  "schemaVersion": 2,
  "provider": "mcp.client",
  "tools": [
    {
      "mcpName": "qbo_contact_search_customer",
      "operation": "quickbooks.customer.search",
      "kind": "tpf:query",
      "majorVersion": 1,
      "input": "QuickBooksCustomerSearchRequest",
      "output": "QuickBooksCustomerSearchResult",
      "inputSchema": {
        "type": "object",
        "additionalProperties": false,
        "properties": { "searchTerm": { "type": "string" } },
        "required": ["searchTerm"]
      },
      "inputSchemaSha256": "sha256:5723da48e6155c443c8901fb2c771948ea54b23121a8f8feb28f924d88d0dc2d",
      "outputSchema": {
        "type": "object",
        "additionalProperties": false,
        "properties": {
          "customers": {
            "type": "array",
            "items": {
              "type": "object",
              "additionalProperties": false,
              "properties": { "id": { "type": "string" }, "displayName": { "type": "string" } },
              "required": ["id", "displayName"]
            }
          }
        },
        "required": ["customers"]
      },
      "outputSchemaSha256": "sha256:f4e552a797380f61962f24473a50bcec2ad0de7d70bec218745414fc9aea1129",
      "projectionId": "tpf-mcp-importer-v1",
      "resultMode": "structured",
      "pinSha256": "sha256:852484ccefb5e87cdd37a415f1186215939050926805affd03ef8212db95324f"
    }
  ]
}
```

Neither resource contains credentials, endpoints, sessions, process handles, or MCP transport
objects.

## Select a narrower input

When an optional external property is unsupported or unnecessary, explicitly choose the fields
your operation accepts. For example, an invoice tool can omit its optional `linked_txn` array:

```xml
<tool>
  <mcpName>create_invoice</mcpName>
  <operation>quickbooks.invoice.create</operation>
  <kind>command</kind>
  <majorVersion>1</majorVersion>
  <inputType>CreateQuickBooksInvoice</inputType>
  <outputType>QuickBooksInvoiceCreated</outputType>
  <includeFields>
    <field>params.customer_id</field>
    <field>params.line_items</field>
    <field>params.due_date</field>
    <field>params.global_tax_calculation</field>
  </includeFields>
</tool>
```

Use the exact discovered names. This example assumes all required fields of your server's
`params` object are included. The resulting request retains its external structure:

```json
{
  "params": {
    "customer_id": "42",
    "line_items": [{ "amount": 125 }],
    "global_tax_calculation": "TaxExcluded"
  }
}
```

Selecting a parent includes its entire subtree. Selecting children keeps only those children;
each selected parent must remain a closed object and retain every required property. Selecting
`params.line_items` keeps its complete item schema and collection bounds. Paths through array
items, renaming, default insertion and computed values are not supported. Duplicate paths,
parent/child overlaps, unknown paths and omitted required properties fail refresh with a path
diagnostic. Paths use dot-separated property names containing letters, digits or underscores,
starting with a letter or underscore. At most 1,024 paths of 1,024 characters each are accepted,
with no more than 64 nested parent selections.

Omitting `includeFields` (or using an empty list) requests the complete input schema. The importer
never silently drops unsupported optional fields. Unsupported constraints on selected parents,
including cross-field dependencies, and unknown schema keywords also fail projection. The
canonical provider metadata and callable schema contain only the selected graph. The private
`mcp-tools.json` entry records the sorted
`includeFields` selection; configuration order does not affect the resources. A changed selection
changes this private resource even if the selected fields happen to have equivalent types.

The generated input retains the selected external field names and nesting, so ordinary typed
serialization sends that structure without a separate transformation. Dynamic operation dispatch
validates against the narrowed canonical contract before invocation; unknown fields are rejected.
Selection does not grant callable exposure: the application must still expose the operation
through its named binding and release catalogue. A discovered-only tool remains unavailable.

Object keys are sorted recursively and equivalent JSON numbers are normalized before hashing.
Array order is preserved, including schema arrays. `pinSha256` hashes the complete tool object
except `pinSha256` itself, so changes to a schema, tool name, operation identity, canonical type
selection, projection, or result mode change the pin. Hashes detect drift; they do not grant authority.

The strict reader rejects unknown fields, duplicate JSON keys, mismatched hashes, and inconsistent
result modes. Identical operation pins from multiple classpath resources coalesce; different pins
for the same kind/operation/major version fail deterministically. Version-1 resources must be
regenerated with an explicit refresh; they cannot verify the original external contract.

Before dispatch, the adapter validates the final argument object against its committed input
schema and field selection, in addition to normal canonical validation. The complete original
schema is retained and hashed, including omitted optional fields. The shared selection algorithm
retains required fields and parent constraints and rejects omitted fields before dispatch, so
unsupported schemas in omitted optional subtrees need not be interpreted. Invalid arguments produce
`mcp-invalid-arguments` without calling the server. The runtime never discovers a replacement schema.

## Tools without an output schema

Omit `<outputType>` when the server does not declare `outputSchema`. Refresh selects the built-in
`<tpf.connector.JsonPayload>` result, contributes only the imported input types, and writes
`"resultMode": "json-payload"` in the private pin. Both `outputSchema` and `outputSchemaSha256`
are absent. A configured output type does not invent a typed contract for an unstructured tool.

The standard provider manifest uses the ordinary type contract:

```json
"typeContract": {
  "input": "QuickBooksCustomerSearchRequest",
  "output": "<tpf.connector.JsonPayload>"
}
```

Use that contributed output directly in your Query step:

```yaml
- name: Read QuickBooks customer data
  kind: query
  cardinality: ONE_TO_ONE
  input: QuickBooksCustomerSearchRequest
  output: <tpf.connector.JsonPayload>
  using: quickbooks
  operation: quickbooks.customer.search
  operationVersion: 1
```

The canonical payload has exactly three string fields:

```json
{
  "contentType": "application/json",
  "schemaHint": "urn:tpf:mcp:call-tool-result:v1",
  "bodyJson": "{\"content\":[{\"text\":\"Customer 42\",\"type\":\"text\"}],\"isError\":false}"
}
```

`bodyJson` is deterministic JSON for the complete MCP `CallToolResult` data received by the SDK.
It preserves content-block order, text, image/audio base64, resource data, annotations, result
metadata, and any optional `structuredContent`; it does not include client/session state. No text
is silently parsed into business fields or discarded. Query capture stores this ordinary result,
and dynamic invocation carries it in `OperationObservation.resultJson`. `LIVE_ONLY` still replays
captured results without reconnecting to MCP.

For declared output schemas, absent or invalid `structuredContent` remains a provider failure.
It never switches to the envelope. MCP `isError` results also remain failures. Commands with an
invalid result after dispatch retain the ordinary ambiguous-effect outcome.

Downstream, parse `bodyJson` with programmatic code to produce a domain type such as
`QuickBooksInvoice`. An explicitly authored LLM Query can handle experimental extraction or
remediation after parsing fails; import and invocation do not run an implicit LLM repair step.

## Importer v1 schema limits

Importer v1 accepts:

- closed object schemas, including nested objects;
- supported canonical scalars;
- optional or nullable non-array fields;
- required, non-null homogeneous arrays;
- `minItems` and `maxItems` on those arrays;
- supported string and numeric wrapper constraints;
- scalar `enum` and `const`, normalized to canonical `allowedValues`.

It rejects open maps, tuples, optional or nullable arrays, references, recursive definitions,
object/array `enum` or `const`, and composition keywords. Failures include the relevant schema path.

Private schemas use a bounded importer-v1 dialect: unknown schema keywords and references fail
refresh rather than being ignored or fetched. Pins accept absent dialect markers, draft 2020-12,
2019-09, or draft-07 markers within this supported subset. Each JSON resource/schema is limited to
1 MiB, nesting depth 64, and 20,000 nodes; at most 128 tool entries and 64 classpath pin resources
are accepted. Arguments and unstructured result JSON use the same size/depth/node limits. Oversized
values fail explicitly rather than being truncated. These bounds protect import and invocation work.
Pinned patterns use [RE2/J](https://github.com/google/re2j) to avoid exponential backtracking;
lookaround and backreferences are rejected, and pattern text/program size is capped at 4,096.
This is an importer restriction and does not change canonical v3 pattern semantics.

These are importer-v1 projection limits, not limitations added to canonical v3. If a QuickBooks
tool cannot be projected losslessly, select a simpler operation or place a deliberately shaped MCP
bridge in front of it. Do not silently discard schema semantics or weaken the canonical contract.

## Configure the runtime binding

Add a named binding in `pipeline.yaml`:

```yaml
connectors:
  quickbooks:
    provider: mcp.client
    version: 1
    config:
      connection: quickbooks-sandbox
```

`quickbooks-sandbox` is a deployment-owned connection reference. The host's `ConnectionResolver`
must turn it into an initialized `McpClientConnection` for the current tenant and invocation.

The deployment must provide:

- authorization through the selected server's supported workflow (Node owns it for local STDIO);
- company/realm and tenant selection;
- MCP client construction and initialization;
- HTTP sessions and transport state;
- STDIO process creation, supervision, restart, and shutdown.

The Connector borrows the initialized client for live invocation and never closes it. Authored
services do not receive QuickBooks credentials or an MCP client.

A Quarkus host can expose the connection through its single application `ConnectionResolver`:

```java
@ApplicationScoped
final class QuickBooksConnectionResolver implements ConnectionResolver {
    private final QuickBooksMcpClients clients;

    QuickBooksConnectionResolver(QuickBooksMcpClients clients) {
        this.clients = clients;
    }

    @Override
    public <C extends ResolvedConnection> CompletionStage<C> resolve(
        ConnectionResolutionRequest<C> request
    ) {
        return clients.resolve(request);
    }
}
```

`QuickBooksMcpClients` is application-host infrastructure, not an authored pipeline service. It
owns initialized-client reuse, transport health checking, and shutdown. Node owns OAuth for the
local STDIO case. If the application has other
authenticated connectors, route all of them behind the same `ConnectionResolver` bean.

The optional [Gmail host connection library](./host-authenticated-connectors.md#durable-gmail-host-connections)
demonstrates durable authorization behind that boundary; it is not a QuickBooks implementation.
For a QuickBooks STDIO server that refreshes and persists its own tokens, keep that server or host
connection infrastructure as the sole refresh authority. Startup environment injection alone does
not preserve rotated tokens across restarts. Hosted HTTP MCP authorization also has its own resource
audience and session requirements; an upstream QuickBooks API token is not automatically an MCP token.
These differences belong in host adapters and do not change imported MCP operation contracts.

### Use the optional local QuickBooks host

`org.pipelineframework:host-quickbooks-mcp` supplies a **local-development STDIO adapter**.
Node owns authorization end-to-end: consent, callbacks, account selection, credentials, refresh,
persistence and reauthorization. Java does not read or validate Node's token files, pass tokens,
inspect realm/client IDs, or impose a credential-storage format.

Configure and authorize the Node server using its own supported workflow. Provide a Node-owned
launcher that starts the approved server instance with its own configuration. The host registers
only an opaque instance identity, command and working directory:

```java
var registration = new QuickBooksRegistration(
    tenantId, new ConnectionRef("quickbooks-sandbox"), "local-qbo-instance",
    List.of("/absolute/path/to/node-owned-launcher"),
    Path.of("/absolute/path/to/local-server-workspace"));

var clients = new QuickBooksMcpClients(
    List.of(registration), hostBlockingExecutor, Duration.ofSeconds(15));
```

Types are in `org.pipelineframework.host.quickbooks`. Expose `clients` as the application's single
`ConnectionResolver`, or route MCP requests to it behind that resolver. Commands and arguments are
trusted local deployment configuration; never put credentials in arguments or obtain commands,
instance IDs or account selection from pipeline payloads.

The launcher owns Node-specific settings, including any token-store location or read-only tool
configuration required by the chosen Intuit server. The Java adapter starts it with an empty
environment; use an absolute executable and have the launcher explicitly provide its environment.
The adapter neither downloads the server nor runs its authorization workflow. It does not invent
a connection-status MCP tool that the server does not support.

Java maps the exact tenant/reference pair to a registered process, initializes and reuses its MCP
client, and pings it before returning `McpClientConnection`. The tenant-to-instance mapping is
host-attested. It is not a claim that Java has verified the connected QuickBooks realm. Ping checks
MCP liveness, not authorization status; authorization failures and recovery belong to Node.

One manager accepts up to 32 registered instances and rejects duplicate instance identities.
Use one manager for these local instances. There is no cross-process lease, account registry,
token-file lock, durable disconnect or production failover mechanism. Do not independently launch
another copy of the same Node instance through the importer or another local client.

After draining invocations, call `clients.close()` on a blocking host thread and then shut down
the executor. The adapter waits for each child to exit, retains uncertain sessions for cleanup,
and prevents stale clients or failed initialization from automatically spawning another process.
Node's private state remains untouched. Upstream stderr is discarded; avoid protocol-body logging
when using real accounts.

Tests use a real STDIO subprocess whose private state is opaque to the adapter. They cover tenant
binding, client reuse, state surviving restart, failed initialization and delayed shutdown. They do
not exercise live Intuit consent. Existing MCP Connector tests prove capture/replay without live
resolution.

The production hosted Streamable HTTP service is a separate integration with different authorization
and session requirements. This local adapter establishes no production OAuth, token forwarding or
connection-management contract. Both still feed the existing initialized `McpClientConnection`
boundary without changing imported operation contracts.

## Invoke it as an ordinary Query

For deterministic pipeline use, declare a normal Query step:

```yaml
- name: Find QuickBooks customer
  kind: query
  cardinality: ONE_TO_ONE
  input: QuickBooksCustomerSearchRequest
  output: QuickBooksCustomerSearchResult
  using: quickbooks
  operation: quickbooks.customer.search
  operationVersion: 1
```

MCP Queries are conservatively imported as `LIVE_ONLY`. This prevents them from becoming a general
cache source, but does not bypass Query observation capture and replay. Replay lookup occurs before
binding activation, connection resolution, or server invocation.

## Expose it to an LLM deliberately

To let an LLM Query propose the operation, list it in that step's release-pinned callable
catalogue:

```yaml
callables:
  findQuickBooksCustomer:
    using: quickbooks
    operation: quickbooks.customer.search
    operationVersion: 1
    kind: query
    input: QuickBooksCustomerSearchRequest
```

An imported operation absent from `callables` is not model-callable. The dynamic operation adapter
rejects an unexposed `binding/operation` pair before provider activation.

The model proposes an ordinary `<tpf.llm.AgentCall>` and receives an ordinary
`<tpf.connector.OperationObservation>`. MCP-specific protocol classes never enter either contract.

## Add write operations as Commands

After the Query path works against a sandbox, a QuickBooks write such as invoice creation can be
imported deliberately:

```xml
<tool>
  <mcpName>qbo_sales_create_invoice</mcpName>
  <operation>quickbooks.invoice.create</operation>
  <kind>command</kind>
  <majorVersion>1</majorVersion>
  <inputType>CreateQuickBooksInvoice</inputType>
  <outputType>QuickBooksInvoiceCreated</outputType>
</tool>
```

If an LLM may select it, expose it with ordinary Command authority and identity:

```yaml
callables:
  createQuickBooksInvoice:
    using: quickbooks
    operation: quickbooks.invoice.create
    operationVersion: 1
    kind: command
    input: CreateQuickBooksInvoice
    commandIdGenerator: com.acme.quickbooks.InvoiceCommandIdGenerator
    duplicatePolicy: RETURN_RECORDED
```

MCP descriptions, prompts, and server-side approval wording are not TPF authority. Command effect
identity, policy, confirmation, idempotency, retry/redrive, and ambiguous-result handling remain
owned by the ordinary TPF Command path.

## Refresh safely

Treat refresh like a dependency or API contract update:

1. Pin the MCP server package, image, or hosted contract version.
2. Refresh against a sandbox or controlled environment.
3. Review both generated resource diffs.
4. Confirm Query versus Command classification.
5. Review canonical type and constraint changes.
6. Compile and run pipeline tests before merging.
7. Change callable exposure separately and deliberately.

A later discovery result never silently reinterprets the committed release. Runtime-varying MCP
discovery and identified callable snapshots are a separate capability; they are not required for
this pinned/static workflow.

See [One-turn LLM Query](./llm-query.md) for callable exposure and
[Host-authenticated Connectors](./host-authenticated-connectors.md) for the runtime connection seam.

External references:

- [Intuit hosted MCP pilot](https://github.com/IntuitDeveloper/intuit-3p-ai-pilot)
- [Intuit QuickBooks Online MCP server](https://github.com/intuit/quickbooks-online-mcp-server)
