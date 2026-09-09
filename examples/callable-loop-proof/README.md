# Callable Loop Proof

This deliberately neutral fixture proves that a packaged Block can own the ordinary typed topology
used by callable and agentic loops:

```text
AgentState -> one-turn LLM Query -> AgentCall -> dynamic Query | Command
           -> OperationObservation -> authored reducer -> AgentState -> pipeline:callable-loop-proof
```

The `complete` decision variant carries an ordinary typed `ApplicationResult` to the terminal step.
The Block artifact owns the state, decision union, reducer, recursion, completion, and callable
catalogue. The consuming application owns only the connector bindings and explicit Command authority,
then invokes `pipeline: callable-loop-proof`.

The offline LLM adapter is stateless with respect to turn ordering. It reads only the canonical
`AgentState.phase` in `LlmTurnRequest.applicationStateJson()` and returns the same proposal for the
same input. `effectScope` and `nextEffectKey` are excluded from model input. The Block independently
copies trusted context for its reducer and injects `nextEffectKey` into the Command arguments, so the
model neither sees nor authors Command identity data. The recorder counts connector calls for
assertions but never influences a decision.

The proof uses the real connector packaging, contributed protocol types, provider-backed Query and
Command runtimes, dynamic operation adapter, generated branch routing, and bounded direct recursion.
It adds no reusable generic-agent API, Agent runtime, dispatch step kind, execution ledger, memory
subsystem, MCP, or Await path.

Run it from the repository root:

```bash
./mvnw -pl examples/callable-loop-proof -am verify -Dmaven.repo.local="$PWD/.m2/repository"
```

Inspect the generated evidence under
`target/classes/META-INF/pipeline/`, especially `pipeline-contract.json`, `order.json`, and
`branching.json`. `pipeline-contract.json` shows the contributed protocol types, dynamic operation
descriptor, qualified imported definition, sanitized callable provenance, and recursive binding;
`order.json` shows the finite root invocation; `branching.json` shows the `call` and `complete`
routes. `connector-bindings.json` contains the pinned callable catalogue as generated inspection
metadata; runtime dispatch uses the descriptor compiled from the linked Block and trusted provider
manifests.
