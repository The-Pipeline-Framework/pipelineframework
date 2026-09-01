CREATE EXTENSION IF NOT EXISTS vector;
CREATE TABLE IF NOT EXISTS rag_vectors (
  item_id text PRIMARY KEY,
  content text NOT NULL,
  embedding vector(768) NOT NULL,
  updated_command_id text NOT NULL
);
CREATE TABLE IF NOT EXISTS rag_vector_commands (
  command_id text PRIMARY KEY,
  request_fingerprint text NOT NULL,
  item_id text NOT NULL
);
CREATE INDEX IF NOT EXISTS rag_vectors_cosine_hnsw
  ON rag_vectors USING hnsw (embedding vector_cosine_ops);
