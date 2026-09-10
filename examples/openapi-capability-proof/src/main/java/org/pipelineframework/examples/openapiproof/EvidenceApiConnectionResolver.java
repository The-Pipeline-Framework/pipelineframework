package org.pipelineframework.examples.openapiproof;

import java.net.URI;
import java.net.http.HttpClient;
import java.util.List;
import java.util.Map;
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

/** Application-owned HTTP authority, client, tenant policy, and already-resolved authorization material. */
@ApplicationScoped
@Unremovable
public class EvidenceApiConnectionResolver implements ConnectionResolver {
    private final URI baseUri;
    private final HttpClient client;

    @Inject
    public EvidenceApiConnectionResolver(@ConfigProperty(name = "openapi.proof.base-uri") URI baseUri) {
        this(baseUri, HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build());
    }

    EvidenceApiConnectionResolver(URI baseUri, HttpClient client) {
        this.baseUri = baseUri;
        this.client = client;
    }

    @Override
    public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
        if (!"evidence-api".equals(request.reference().value())) {
            return CompletableFuture.failedStage(new IllegalArgumentException(
                "Unknown HTTP connection: " + request.reference().value()));
        }
        if (request.invocationContext().tenantId().isEmpty()) {
            return CompletableFuture.failedStage(new IllegalArgumentException(
                "OpenAPI proof requires an invocation tenant"));
        }
        HttpClientConnection connection = new HttpClientConnection(client, baseUri, Set.of("oauth2", "apiKey"),
            authorization -> CompletableFuture.completedStage(authorization.security().requirements().stream()
                .anyMatch(requirement -> requirement.scheme().equals("oauth2"))
                    ? new HttpAuthorizationMaterial(Map.of("Authorization", List.of("Bearer host-resolved")),
                        Map.of(), Map.of())
                    : new HttpAuthorizationMaterial(Map.of("X-Api-Key", List.of("host-resolved")),
                        Map.of(), Map.of())));
        return CompletableFuture.completedStage(request.connectionType().cast(connection));
    }
}
