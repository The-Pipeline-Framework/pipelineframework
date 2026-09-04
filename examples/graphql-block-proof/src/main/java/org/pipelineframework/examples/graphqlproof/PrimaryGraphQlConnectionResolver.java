package org.pipelineframework.examples.graphqlproof;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.smallrye.graphql.client.GraphQLClient;
import io.smallrye.graphql.client.dynamic.api.DynamicGraphQLClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ResolvedConnection;
import org.pipelineframework.connector.graphql.smallrye.AuthenticatedGraphQlConnection;

/** Application-owned connection resolver: client configuration and tenant policy stay outside the Connector. */
@ApplicationScoped
public class PrimaryGraphQlConnectionResolver implements ConnectionResolver {
    private final DynamicGraphQLClient client;

    @Inject
    public PrimaryGraphQlConnectionResolver(
        @GraphQLClient("primary-graphql") DynamicGraphQLClient client
    ) {
        this.client = Objects.requireNonNull(client, "GraphQL client must not be null");
    }

    @Override
    public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
        if (!request.reference().value().equals("primary-graphql")) {
            return CompletableFuture.failedStage(
                new IllegalArgumentException("Unknown GraphQL connection: " + request.reference().value()));
        }
        if (request.invocationContext().tenantId().isEmpty()) {
            return CompletableFuture.failedStage(
                new IllegalArgumentException("GraphQL proof requires an invocation tenant"));
        }
        return CompletableFuture.completedStage(
            request.connectionType().cast(new AuthenticatedGraphQlConnection(client)));
    }
}
