package org.pipelineframework.objectpublish;

/**
 * Adapts terminal transport objects back to the domain type consumed by Object Publish.
 */
public interface TerminalOutputAdapter<T, D> {
    Class<D> domainType();

    /**
     * Converts a non-null generated terminal transport item into the domain type consumed by Object Publish.
     *
     * @param item non-null transport terminal output
     * @return non-null domain item consumed by the configured Object Publish mapper
     * @throws IllegalArgumentException if the transport item cannot be converted to the domain type
     * @throws NullPointerException if {@code item} is null
     */
    D toDomain(T item);
}
