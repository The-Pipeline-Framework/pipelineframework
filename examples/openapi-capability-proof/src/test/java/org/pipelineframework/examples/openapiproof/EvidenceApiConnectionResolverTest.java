package org.pipelineframework.examples.openapiproof;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.http.HttpClient;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.http.HttpClientConnection;

class EvidenceApiConnectionResolverTest {
    @Test
    void hostSelectsAuthorityClientAndTenant() {
        HttpClient client = mock(HttpClient.class);
        when(client.followRedirects()).thenReturn(HttpClient.Redirect.NEVER);
        var resolver = new EvidenceApiConnectionResolver(URI.create("https://runtime.example/v1"), client);
        var request = new ConnectionResolutionRequest<>(new ConnectionRef("evidence-api"),
            HttpClientConnection.class, context(Optional.of("tenant-a")));

        HttpClientConnection connection = resolver.resolve(request).toCompletableFuture().join();

        assertEquals(URI.create("https://runtime.example/v1"), connection.baseUri());
        assertEquals(client, connection.client());
    }

    @Test
    void rejectsMissingTenantBeforeReturningAuthority() {
        HttpClient client = mock(HttpClient.class);
        when(client.followRedirects()).thenReturn(HttpClient.Redirect.NEVER);
        var resolver = new EvidenceApiConnectionResolver(URI.create("https://runtime.example"), client);
        var request = new ConnectionResolutionRequest<>(new ConnectionRef("evidence-api"),
            HttpClientConnection.class, context(Optional.empty()));

        assertThrows(Exception.class, () -> resolver.resolve(request).toCompletableFuture().join());
    }

    private static ConnectorExecutionContext context(Optional<String> tenant) {
        return new ConnectorExecutionContext(tenant, Optional.of("execution-1"), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }
}
