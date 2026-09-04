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
  "schemaVersion": 5,
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

`mcp-tools.json` preserves the exact external tool name needed by the MCP adapter. It is not a
second public operation schema:

```json
{
  "schemaVersion": 1,
  "provider": "mcp.client",
  "tools": [
    {
      "mcpName": "qbo_contact_search_customer",
      "operation": "quickbooks.customer.search",
      "kind": "tpf:query",
      "majorVersion": 1,
      "input": "QuickBooksCustomerSearchRequest",
      "output": "QuickBooksCustomerSearchResult"
    }
  ]
}
```

Neither resource contains credentials, endpoints, sessions, process handles, or MCP transport
objects.

## Importer v1 schema limits

Importer v1 accepts:

- closed object schemas, including nested objects;
- supported canonical scalars;
- optional or nullable non-array fields;
- required, non-null homogeneous arrays;
- supported string and numeric wrapper constraints.

It rejects open maps, tuples, optional or nullable arrays, references, recursive definitions,
enums, and composition keywords. Failures include the relevant schema path.

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

The host owns:

- QuickBooks OAuth and token renewal;
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
        String tenantId = request.invocationContext().tenantId().orElseThrow();
        return clients.initializedClient(tenantId, request.reference())
            .thenApply(McpClientConnection::new)
            .thenApply(request.connectionType()::cast);
    }
}
```

`QuickBooksMcpClients` is application-host infrastructure, not an authored pipeline service. It
owns OAuth, initialized-client reuse, health checking, and shutdown. If the application has other
authenticated connectors, route all of them behind the same `ConnectionResolver` bean.

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
