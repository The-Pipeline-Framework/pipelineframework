package org.pipelineframework.examples.openapiproof;

import java.net.URI;
import java.net.http.HttpClient;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ResolvedConnection;
import org.pipelineframework.connector.http.HttpAuthorizationMaterial;
import org.pipelineframework.connector.http.HttpClientConnection;

/** The host owns provider origin and client; callback authentication is configured separately. */
@ApplicationScoped
@Unremovable
public class JobConnectionResolver implements ConnectionResolver {
    private final Optional<URI> base;
    private final HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();

    @Inject
    public JobConnectionResolver(@ConfigProperty(name = "openapi.proof.jobs-base-uri") Optional<URI> base) {
        this.base = base;
    }

    @jakarta.annotation.PreDestroy
    void closeClient() { client.close(); }

    @Override
    public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
        if (!"jobs-api".equals(request.reference().value()) || request.invocationContext().tenantId().isEmpty()) {
            return CompletableFuture.failedStage(new IllegalArgumentException("Unknown job connection or tenant"));
        }
        URI origin = base.orElseThrow(() -> new IllegalStateException("Configure the jobs provider base URI"));
        HttpClientConnection connection = new HttpClientConnection(client, origin, Set.of(),
            authorization -> CompletableFuture.completedStage(
                new HttpAuthorizationMaterial(Map.of(), Map.of(), Map.of())));
        return CompletableFuture.completedStage(request.connectionType().cast(connection));
    }
}
