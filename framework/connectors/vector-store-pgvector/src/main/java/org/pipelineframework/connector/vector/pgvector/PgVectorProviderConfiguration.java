package org.pipelineframework.connector.vector.pgvector;

/** Binding-owned vector dimensionality for one pgvector index. */
public record PgVectorProviderConfiguration(Integer dimensions) {
    public PgVectorProviderConfiguration {
        if (dimensions == null || dimensions <= 0) {
            throw new IllegalArgumentException("pgvector dimensions must be positive");
        }
    }
}
