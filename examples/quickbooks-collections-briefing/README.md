# QuickBooks collections briefing

This reference application asks a useful operational question: **who should collections call first
today, and what should we do next?** It reads QuickBooks Online's aged-receivables report through
one explicitly imported MCP tool and turns the response into a typed, priority-ordered collections
intervention plan.

```text
QuickBooksAgedReceivablesRequest
    -> pinned quickbooks.receivables.aged Query
    -> captured <tpf.connector.JsonPayload>
    -> ONE_TO_MANY deterministic report projection
    -> ONE_TO_ONE policy per customer
    -> MANY_TO_ONE briefing reduction
    -> CollectionsBriefing
```

That topology is the point of the example. One external stimulus creates one root execution and one
external observation. TPF then owns Query capture/replay, typed fan-out and item lineage, independent
per-customer processing, backpressure, final aggregation, telemetry, and canonical input/output
validation. The application code owns only deterministic report interpretation and collections policy.
There are no hidden nested executions, write-side QuickBooks calls, or injected clients in business
steps.

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

./mvnw -f examples/quickbooks-collections-briefing/app/pom.xml quarkus:run \
  -Dquarkus.args="2026-09-08" \
  -Dmaven.repo.local="$PWD/.m2/repository"
```

The `-f` invocation is intentional: `quarkus:run` must target the application module, not every
aggregator selected by `-am`. The optional command argument is the report date (today by default).
The command enters through TPF's root execution service and prints the typed action plan. Its request
is the canonical imported type. For example:

```json
{
  "params": {
    "aging_method": "Report_Date",
    "days_per_aging_period": 30,
    "num_periods": 4,
    "report_date": "2026-09-08"
  }
}
```

The captured sandbox scenario for `2026-09-08` produces:

```text
4 customers owe GBP 33860.82; GBP 33860.82 is overdue. 2 need priority contact;
first: Abercrombie International Group (critical, GBP 30620.82 overdue).

  CRITICAL Abercrombie International Group  overdue GBP 30620.82
           Call today, confirm the balance details, and agree a dated payment plan.
  HIGH     Adwin Ko                         overdue GBP 1800.00
           Contact within one business day and request a firm payment date.
  STANDARD Benjamin Yeung                   overdue GBP 960.00
           Send a personalised reminder and review again in seven days.
  STANDARD Jordan Burgess                   overdue GBP 480.00
           Send a personalised reminder and review again in seven days.
```

## Fixture and reproducible proof

`app/src/test/resources/fixtures/qbo-sandbox-aged-receivables-2026-09-08.json` is a captured response
from the sandbox company, with only the volatile response timestamp removed. It is test data, not a
production-company export. The fixture retains the real QuickBooks column metadata, customer IDs,
summary row, blank aging buckets, currency, and decimal values so schema-shape drift remains visible.

The companion `qbo-empty-aged-receivables.json` fixture proves that an empty report preserves its
date and currency through fan-out and reduces to a zero-account briefing. The unit tests prove
deterministic projection and all policy lanes. The Quarkus integration tests use
the same fixture behind a mocked MCP transport to prove the complete four-step pipeline, release-pinned
dispatch, Query capture/replay without a second provider call, and secret/session isolation. A
replay-enabled scenario additionally asserts that one external input emits exactly one root replay
file containing all four authored stages.

The checked-in `demo-execution.json` records the named scenario, root-execution budget, request, and
expected business result. To refresh it, run the live sandbox command above, compare the output to a
fresh provider response, then deliberately update both the fixture and dataset in one reviewable
change.

The MCP tool has no declared `outputSchema`, so TPF preserves the complete result as
`<tpf.connector.JsonPayload>`. `AgedReceivablesInterpreterService` uses deterministic application
code to recognize the report and emit one typed `CollectionAccount` per customer.
`CollectionPolicyService` independently assigns an intervention lane and action to each account;
`CollectionsBriefingService` orders and reduces those actions into the final typed result. This is
the normal fast path. An author can add an explicit LLM Query as a remediation branch for payloads
that fail this parser; the importer and connector never invoke an LLM implicitly.

Because the MCP operation is an ordinary TPF Query, a captured result is replayed through normal
Query semantics without reconnecting to the STDIO server. `LIVE_ONLY` describes provider
cacheability; it does not bypass capture/replay.

## Productionization notes

This is a safe read-only demonstration, not a collections system of record. A production application
would externalize policy thresholds, map customer IDs to an authorized case-management boundary,
define policy for multi-currency reporting, and set retention/redaction policy
for Query captures and replay files. Any future QuickBooks write must be imported and modeled as a
TPF Command with explicit authorization, confirmation, idempotency, and ambiguous-outcome handling;
it must not be smuggled into this Query or a business service.
