# Turnkey two-application RAG

This reference topology is two independently deployable TPF applications. The queue-async INDEXER admits text objects and runs `Document → parse → ONE_TO_MANY chunks → embedding Query → vector upsert Command → MANY_TO_ONE IndexReceipt`. The synchronous REST QUERY application runs `Question → embedding Query → vector search Query → RetrievedContext → LLM Query → citation validation → Answer`.

They share only the embedding model and durable pgvector tables. They have separate roots, processes, datasource pools, capture/effect stores, retries, scaling, releases, failure domains, admission mechanisms, and latency objectives. Querying is not the next stage of one indexing execution, so checkpoint handoff is inappropriate.

Start infrastructure with `scripts/infrastructure.sh up -d`, pull models with `scripts/pull-models.sh`, and run `scripts/run-indexer.sh` and `scripts/run-query.sh` in separate terminals. Drop a UTF-8 text file with `scripts/ingest.sh path/to/file.txt`; ask with `scripts/ask.sh 'your question'`. PostgreSQL listens on 5433 and Ollama on 11435 to avoid common local defaults.

`EmbeddingProviderConfiguration.model` and dimensions are binding-owned because they define vector meaning. Ollama endpoints/timeouts and PostgreSQL connection/schema/table/pool settings are deployment configuration. The existing LLM Ollama binding retains its established binding-level `baseUrl`.

Chunk IDs encode immutable source provenance, index, and content hash. The model authors only answer text and cited chunk IDs. The final validator rejects unknown, duplicate, or missing citations and copies source IDs/excerpts only from retrieved typed context.

The sibling `rag-composition-proof` remains the deterministic, network-free CI fixture. Its shared in-memory binding is a proof convenience, not the recommended deployment topology.
