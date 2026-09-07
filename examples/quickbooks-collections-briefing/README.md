# QuickBooks collections briefing

This reference application asks a useful operational question: **who should collections call first
today?** It reads QuickBooks Online's aged-receivables report through one explicitly imported MCP
tool and turns the response into a typed, priority-ordered `CollectionsBriefing`.

```text
QuickBooksAgedReceivablesRequest
    -> pinned quickbooks.receivables.aged Query
    -> captured <tpf.connector.JsonPayload>
    -> deterministic Jackson interpretation
    -> CollectionsBriefing
```

The example deliberately separates three states: the MCP server discovers many tools, the committed
resources import one of them, and `pipeline.yaml` makes that one operation callable through the
named `quickbooks` binding. The build never contacts QuickBooks. The checked-in import contains
schemas and hashes, but no credentials, process handles, or MCP session state.

## Refresh the explicit import

Set the server path and QuickBooks credentials in the host environment, then run the importer goal
explicitly. The plugin configuration in `contracts/pom.xml` selects only `get_aged_receivables` and narrows its
optional input fields.

```bash
export QUICKBOOKS_MCP_SERVER=/absolute/path/to/quickbooks-online-mcp-server/dist/index.js
export QUICKBOOKS_CLIENT_ID=...
export QUICKBOOKS_CLIENT_SECRET=...
export QUICKBOOKS_REFRESH_TOKEN=...
export QUICKBOOKS_REALM_ID=...
export QUICKBOOKS_ENVIRONMENT=sandbox

./mvnw -pl examples/quickbooks-collections-briefing/contracts \
  org.pipelineframework:connector-mcp-maven-plugin:refresh-import \
  -Dquickbooks.mcp.server="$QUICKBOOKS_MCP_SERVER" \
  -Dmaven.repo.local="$PWD/.m2/repository"
```

Review and commit both generated resources together:

- `contracts/src/main/resources/META-INF/pipeline/connector-providers.json` is ordinary public Connector
  metadata used by compilation and release-pinned exposure.
- `contracts/src/main/resources/META-INF/pipeline/mcp-tools.json` is the private invocation pin used by the
  MCP adapter for schema validation and exact tool dispatch.

Do not edit either file by hand.

## Build and run

There is one authored `pipeline.yaml`. Its `quickbooks` binding refers to the deployment-owned
`quickbooks-sandbox` connection. The matching definition is explicit in `application.properties`:
Any `target/classes/pipeline.yaml` seen after a build is only a generated classpath copy and must
not be edited.

```properties
quickbooks.connection.reference=quickbooks-sandbox
quickbooks.connection.tenant=sandbox-company
quickbooks.connection.server-instance=local-quickbooks-sandbox
quickbooks.mcp.node=${QUICKBOOKS_NODE:}
quickbooks.mcp.server=${QUICKBOOKS_MCP_SERVER:}
quickbooks.mcp.working-directory=${QUICKBOOKS_MCP_WORKING_DIRECTORY:}
```

The YAML owns the portable binding and logical connection reference; deployment configuration owns
the executable, working directory, tenant-to-instance registration, and process lifecycle. The app
uses TPF's `host-quickbooks-mcp` component instead of implementing an MCP client lifecycle itself.
It starts the Node process only when the Query first resolves the connection and closes the client,
transport, and STDIO process when the application stops. The child receives an empty environment;
the Node server remains the sole owner of OAuth and loads its own local configuration.

```bash
export QUICKBOOKS_NODE="$(command -v node)"
export QUICKBOOKS_MCP_SERVER=/absolute/path/to/quickbooks-online-mcp-server/dist/index.js
# Optional; defaults to the parent of dist/ for a dist/index.js entry point.
export QUICKBOOKS_MCP_WORKING_DIRECTORY=/absolute/path/to/quickbooks-online-mcp-server

./mvnw -pl examples/quickbooks-collections-briefing -am verify \
  -Dmaven.repo.local="$PWD/.m2/repository"

./mvnw -pl examples/quickbooks-collections-briefing/app -am quarkus:run \
  -Dquarkus.args="2026-09-07" \
  -Dmaven.repo.local="$PWD/.m2/repository"
```

The optional command argument is the report date (today by default). The command prints a concise
headline followed by customers ordered by overdue balance. Its request is the canonical imported
type. For example:

```json
{
  "params": {
    "aging_method": "Report_Date",
    "days_per_aging_period": 30,
    "num_periods": 4,
    "report_date": "2026-09-07"
  }
}
```

The MCP tool has no declared `outputSchema`, so TPF preserves the complete result as
`<tpf.connector.JsonPayload>`. `AgedReceivablesInterpreterService` then uses deterministic
application code to recognize the report, map its columns, total the aging buckets, and order
customers by overdue balance. This is the normal fast path. An author can add an explicit LLM Query
as a remediation branch for payloads that fail this parser; the importer and connector never invoke
an LLM implicitly.

Because the MCP operation is an ordinary TPF Query, a captured result is replayed through normal
Query semantics without reconnecting to the STDIO server. `LIVE_ONLY` describes provider
cacheability; it does not bypass capture/replay.
