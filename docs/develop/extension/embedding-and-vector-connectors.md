# Embedding and vector connectors

TPF composes retrieval-augmented generation from existing step semantics. Authored services transform and fan out data, an embedding `Query` observes a model, a vector `Command` records an indexing effect, a vector `Query` observes retrieval, and the existing one-turn LLM `Query` produces the answer. There is no RAG runtime or retriever step kind.

## Portable contracts

Add the provider-neutral contract artifacts to the connector provider or application that needs them:

```xml
<dependency>
  <groupId>org.pipelineframework</groupId>
  <artifactId>embedding-query-connector</artifactId>
  <version>${pipelineframework.version}</version>
</dependency>
<dependency>
  <groupId>org.pipelineframework</groupId>
  <artifactId>vector-store-connector</artifactId>
  <version>${pipelineframework.version}</version>
</dependency>
```

`embedding-query-connector` contributes these canonical types in `tpf.embedding`:

- `EmbeddingRequest(itemId, text)`
- `EmbeddingResult(itemId, text, repeated float32 values)`

Its version 1 `embed` operation is a cacheable, unary `Query`. `itemId` and `text` are caller-owned continuation data and a provider copies them unchanged. The numeric `values` are the external observation.

`vector-store-connector` contributes these types in `tpf.vector`:

- `VectorUpsertRequest(itemId, content, repeated float32 values)`
- `VectorUpsertResult(itemId)`
- `VectorSearchRequest(queryId, queryText, repeated float32 values, limit)`
- `VectorMatch(itemId, content, float32 score)`
- `VectorSearchResult(queryId, queryText, repeated VectorMatch matches)`

Its version 1 `upsert` operation is a `Command`; its version 1 cacheable `search` operation is a `Query`. Vectors must be non-empty and finite, and `limit` must be positive. Search returns at most `limit` matches in descending score order, with ascending `itemId` as the tie-breaker. No matches is a successful result with an empty list. Scores only order one response; they are not portable measurements across models or providers.

The contracts deliberately omit namespaces, collections, filters, metadata predicates, sparse or hybrid search, reranking, and metric selection.

## Configure bindings, not requests

The embedding binding owns model identity because the model changes vector meaning and replay identity. It may also own an optional dimension override and a logical `ConnectionRef`:

```yaml
connectors:
  embedder:
    provider: acme.embedding
    version: 1
    config:
      model: text-embedding-v3
      dimensions: 1024
      connection: embedding-production

  vectors:
    provider: acme.vector
    version: 1
    config:
      collection: product-help
      connection: vector-production
```

The vector provider defines its own binding schema for its target. Endpoints, credentials, client and connection-pool tuning, retry settings, process management, batching windows, and transport timeouts remain host/runtime configuration. Pipeline streaming cardinality supplies batching and backpressure; the portable API does not add a batch operation. Existing execution deadlines and resilience policy own operation timeouts.

## Compose indexing and retrieval

```yaml
- name: Embed chunk
  kind: query
  input: <tpf.embedding.EmbeddingRequest>
  output: <tpf.embedding.EmbeddingResult>
  using: embedder
  operation: embed
  operationVersion: 1
  capture: { keyFields: [itemId, text] }

- name: Upsert vector
  kind: command
  input: <tpf.vector.VectorUpsertRequest>
  output: <tpf.vector.VectorUpsertResult>
  using: vectors
  operation: upsert
  operationVersion: 1
  commandIdGenerator: example.VectorUpsertCommandIdGenerator
  duplicatePolicy: RETURN_RECORDED

- name: Search vectors
  kind: query
  input: <tpf.vector.VectorSearchRequest>
  output: <tpf.vector.VectorSearchResult>
  using: vectors
  operation: search
  operationVersion: 1
  capture: { keyFields: [queryId, queryText, values, limit] }
```

Query capture records one external observation for a stable execution and capture key. It is not an embedding cache, and a cross-execution cache remains a separate policy. Command effect storage records logical effect authority and replay. A stable upsert command ID should include the operation namespace, item identity, content, and an unambiguous encoding of every vector value. Provider idempotency remains complementary: it protects dispatch at the external boundary but does not replace the TPF effect record.

Chunking stays application-authored. A typical index flow uses a deterministic `ONE_TO_MANY` service to produce stable chunk IDs, embeds each chunk through the Query, dispatches one Command per chunk, and uses `MANY_TO_ONE` only when the application needs an indexing receipt. Retrieval embeds the question, searches through a Query, authors a typed context, then passes that value to the [one-turn LLM Query](./llm-query.md).

## Offline composition proof

[`examples/rag-composition-proof`](https://github.com/The-Pipeline-Framework/pipelineframework/tree/main/examples/rag-composition-proof) provides deterministic `proof.embedding`, `proof.vector`, and `proof.rag.llm` providers. It hashes normalized tokens into a small numeric vector, stores immutable entries in a binding-local in-memory index, performs deterministic cosine search, and generates an answer without network, model, database, filesystem, or process access.

The proof is intentionally not a production embedding or vector-database adapter. Its in-memory index lasts only for the configured provider instance. INDEX is the queue-async application root, and the integration test runs the independently authored RETRIEVE definition through its generated step clients in the same runtime so both operations share that binding instance. The proof does not put them behind sibling union routes: a stream-scoped `MANY_TO_ONE` child cannot currently participate in per-item `accepts` routing, and this ecosystem example does not redefine that core behavior.

## Relationship to agentic composition

Basic RAG is independent of an agent loop. A future application may expose vector search as one authorized Query capability alongside JPA or HTTP:

```text
ResearchState
→ LLM Query decision
→ authorized vector Query / JPA Query / HTTP Query
→ OperationObservation
→ authored reducer
→ bounded recursion
```

That composition uses the existing one-turn decision, dynamic operation, observation, reducer, and recursion pattern. It does not require vector search to know about agents, memory, or a RAG runtime.
