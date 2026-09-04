package org.pipelineframework.examples.graphqlproof;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.Optional;

import io.smallrye.graphql.client.dynamic.api.DynamicGraphQLClient;
import org.junit.jupiter.api.Test;

import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.graphql.smallrye.AuthenticatedGraphQlConnection;

class PrimaryGraphQlConnectionResolverTest {
    @Test void retainsConnectionAndTenantAuthorityInTheApplication() {
        DynamicGraphQLClient client = mock(DynamicGraphQLClient.class);
        var resolver = new PrimaryGraphQlConnectionResolver(client);
        var context = new ConnectorExecutionContext(Optional.of("tenant-a"), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
            Optional.empty());

        var connection = resolver.resolve(new ConnectionResolutionRequest<>(
            new ConnectionRef("primary-graphql"), AuthenticatedGraphQlConnection.class, context))
            .toCompletableFuture().join();

        assertEquals(AuthenticatedGraphQlConnection.class, connection.getClass());
        assertThrows(Exception.class, () -> resolver.resolve(new ConnectionResolutionRequest<>(
            new ConnectionRef("other"), AuthenticatedGraphQlConnection.class, context)).toCompletableFuture().join());
        assertThrows(Exception.class, () -> resolver.resolve(new ConnectionResolutionRequest<>(
            new ConnectionRef("primary-graphql"), AuthenticatedGraphQlConnection.class,
            ConnectorExecutionContext.empty())).toCompletableFuture().join());
    }
}
