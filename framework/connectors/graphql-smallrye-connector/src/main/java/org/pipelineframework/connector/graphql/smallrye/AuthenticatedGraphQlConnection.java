package org.pipelineframework.connector.graphql.smallrye;

import java.util.Objects;

import io.smallrye.graphql.client.dynamic.api.DynamicGraphQLClient;
import org.pipelineframework.connector.ResolvedConnection;

/** Runtime-only authenticated client borrowed from the consuming application's connection resolver. */
public final class AuthenticatedGraphQlConnection implements ResolvedConnection {
    private final DynamicGraphQLClient client;

    public AuthenticatedGraphQlConnection(DynamicGraphQLClient client) {
        this.client = Objects.requireNonNull(client, "authenticated GraphQL client must not be null");
    }

    DynamicGraphQLClient client() {
        return client;
    }
}
