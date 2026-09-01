package org.pipelineframework.connector.vector.pgvector;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/** Deployment-owned pgvector schema and table selection. */
@ConfigMapping(prefix = "pipeline.vector.pgvector")
public interface PgVectorRuntimeConfiguration {
    /** Database schema containing the vector and command-ledger tables. */
    @WithDefault("public")
    String schema();

    /** Table containing the indexed vector rows. */
    @WithDefault("rag_vectors")
    String table();

    /** Table containing durable provider command receipts. */
    @WithDefault("rag_vector_commands")
    String commandTable();
}
