# Agent Composition Proof

This framework-owned reference proves that agentic behavior is ordinary typed TPF composition:

```text
AgentState -> one-turn LLM Query -> AgentCall -> dynamic Query | Command
           -> OperationObservation -> authored reducer -> AgentState -> pipeline:agent-loop
```

The `complete` decision variant carries an ordinary typed `ApplicationResult` to the terminal step.

The offline LLM adapter is stateless with respect to turn ordering. It reads only the canonical
`AgentState.phase` in `LlmTurnRequest.applicationStateJson()` and returns the same proposal for the
same input. Its recorder counts invocations for assertions but never influences a decision. Only
`ObservationReducerService` advances the application-owned phase.

The proof uses the real connector packaging, contributed protocol types, provider-backed Query and
Command runtimes, dynamic operation adapter, generated branch routing, and bounded direct recursion.
It adds no Agent runtime, dispatch step kind, execution ledger, memory subsystem, MCP, or Await path.

Run it from the repository root:

```bash
./mvnw -pl examples/agent-composition-proof -am verify -Dmaven.repo.local="$PWD/.m2/repository"
```

Inspect the generated evidence under
`target/classes/META-INF/pipeline/`, especially `pipeline-contract.json`, `order.json`, and
`branching.json`. `pipeline-contract.json` shows the contributed protocol types, dynamic operation
descriptor, and recursive binding; `order.json` shows the finite root invocation; `branching.json`
shows the `call` and `complete` routes. The existing `connector-bindings.json` contains the pinned
callable catalogue as generated inspection metadata; runtime dispatch resolves the same catalogue
from the compiled YAML and trusted provider manifests.
