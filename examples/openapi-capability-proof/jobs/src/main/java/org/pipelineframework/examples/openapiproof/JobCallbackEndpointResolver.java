package org.pipelineframework.examples.openapiproof;

import java.net.URI;
import java.util.Optional;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pipelineframework.connector.ProviderCallbackEndpointResolver;
import org.pipelineframework.connector.ProviderCallbackRequest;

/** Host deployment authority supplies only the public base; TPF appends its signed callback route. */
@ApplicationScoped
@Unremovable
public class JobCallbackEndpointResolver implements ProviderCallbackEndpointResolver {
    private final Optional<URI> publicBase;

    @Inject
    public JobCallbackEndpointResolver(
        @ConfigProperty(name = "openapi.proof.callback-base-uri") Optional<URI> publicBase) {
        this.publicBase = publicBase;
    }

    @Override
    public URI resolve(ProviderCallbackRequest request) {
        return publicBase.orElseThrow(() -> new IllegalStateException("Configure the public callback base URI"));
    }
}
