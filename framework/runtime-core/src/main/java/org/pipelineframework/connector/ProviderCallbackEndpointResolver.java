package org.pipelineframework.connector;

import java.net.URI;

/** Resolves the trusted public base URI. The framework appends its route and signed token. */
@FunctionalInterface
public interface ProviderCallbackEndpointResolver {
    URI resolve(ProviderCallbackRequest request);
}
