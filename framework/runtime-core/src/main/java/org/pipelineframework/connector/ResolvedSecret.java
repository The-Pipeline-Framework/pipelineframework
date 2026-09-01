package org.pipelineframework.connector;

/**
 * Legacy runtime-only secret handle marker.
 *
 * @deprecated Connector authentication must use a typed {@link ResolvedConnection}. A resolved
 * secret has no tenant or invocation context and must not be used for authenticated external
 * connector access.
 */
@Deprecated(forRemoval = true)
public interface ResolvedSecret {
}
