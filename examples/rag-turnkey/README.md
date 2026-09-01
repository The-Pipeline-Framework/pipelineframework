# Turnkey two-application RAG

This reference topology is two independently deployable TPF applications. The queue-async INDEXER admits text objects and runs `Document → parse → ONE_TO_MANY chunks → embedding Query → vector upsert Command → MANY_TO_ONE IndexReceipt`. The synchronous REST QUERY application runs `Question → embedding Query → vector search Query → RetrievedContext → LLM Query → citation validation → Answer`.

They share only external infrastructure: one Ollama service supplies embeddings to both applications and answer generation to QUERY, while PostgreSQL supplies the durable pgvector tables. They have separate roots, processes, datasource pools, connector instances, capture/effect stores, retries, scaling, releases, failure domains, admission mechanisms, and latency objectives. Querying is not the next stage of one indexing execution, so checkpoint handoff is inappropriate.

Start infrastructure with `scripts/infrastructure.sh up -d`, pull models with `scripts/pull-models.sh`, and run `scripts/run-indexer.sh` and `scripts/run-query.sh` in separate terminals. Drop a UTF-8 text file with `scripts/ingest.sh path/to/file.txt`; ask with `scripts/ask.sh 'your question'`. PostgreSQL listens on 5433 and Ollama on 11435 to avoid common local defaults. INDEXER uses HTTP/gRPC ports 8080/9000; QUERY uses 8081/9001. Override them with `INDEXER_HTTP_PORT`, `INDEXER_GRPC_PORT`, `QUERY_HTTP_PORT`, and `QUERY_GRPC_PORT`.

QUERY allows two minutes per local Ollama request because loading the answer model after the embedding model can be slow on a constrained workstation. Override this with `OLLAMA_REQUEST_TIMEOUT`; the ask script bounds connection establishment but deliberately does not impose a total response deadline.

`EmbeddingProviderConfiguration.model` and dimensions are binding-owned because they define vector meaning. Ollama endpoints/timeouts and PostgreSQL connection/schema/table/pool settings are deployment configuration. The Ollama LLM adapter still accepts its established binding-level `baseUrl` for compatibility; when omitted, as in QUERY, it uses the runtime-owned `pipeline.llm.langchain4j.ollama.base-url`. Both Ollama adapters use `OLLAMA_BASE_URL` in this example.

Chunk IDs encode immutable source provenance, index, and content hash. The model authors only answer text and cited chunk IDs. The final validator rejects unknown, duplicate, or missing citations and copies source IDs/excerpts only from retrieved typed context.

The sibling `rag-composition-proof` remains the deterministic, network-free CI fixture. Its shared in-memory binding is a proof convenience, not the recommended deployment topology.
