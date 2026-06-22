package org.pipelineframework.orchestrator;

/**
 * Resolves local control-plane secret references.
 */
public interface ControlPlaneSecretResolver {

    /**
     * Resolves a secret reference into a secret value.
     *
     * @param reference secret reference such as env:NAME, sys:name, or config:key
     * @return resolved secret
     */
    String resolve(String reference);
}
