# RAG composition proof

This offline v3 example proves that modern RAG is ordinary TPF composition:

```text
INDEX
Document → authored ONE_TO_MANY chunking → embedding Query
         → vector upsert Command → MANY_TO_ONE IndexReceipt

RETRIEVE
Question → embedding Query → vector search Query → authored RetrievedContext
         → existing one-turn LLM Query → Answer
```

The two named pipelines use the same configured `proof.vector` provider instance. The embedding provider derives an eight-value vector from normalized token hashes, the vector provider holds an in-memory cosine index, and the LLM provider answers from the retrieved passages. None performs network, filesystem, database, model, or process access.

Run the proof from the repository root:

```bash
./mvnw -pl examples/rag-composition-proof verify \
  -Dmaven.repo.local="$PWD/.m2/repository"
```

The integration test proves generated Query capture and Command effect replay, real `ONE_TO_MANY` command dispatch, bounded ordered search, typed LLM completion, and connector/release metadata.

INDEX is the queue-async application root so every vector write crosses the real Command boundary. The integration test then runs the independently authored RETRIEVE definition through its generated step clients in the same Quarkus runtime, preserving the shared binding-owned store. The example deliberately does not place both behind sibling union routes: a stream-scoped `MANY_TO_ONE` child cannot currently participate in per-item `accepts` routing, and this ecosystem proof does not redefine that core behavior.

This is a proof adapter, not a production vector store. It deliberately has no metadata/filter DSL, namespaces, sparse or hybrid search, reranking, model process, or persistent index.
