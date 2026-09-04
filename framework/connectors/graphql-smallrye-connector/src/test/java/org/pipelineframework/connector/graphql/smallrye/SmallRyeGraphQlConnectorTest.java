package org.pipelineframework.connector.graphql.smallrye;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletionException;

import io.smallrye.graphql.client.GraphQLError;
import io.smallrye.graphql.client.InvalidResponseException;
import io.smallrye.graphql.client.Response;
import io.smallrye.graphql.client.dynamic.api.DynamicGraphQLClient;
import io.smallrye.mutiny.Uni;
import jakarta.json.JsonObject;
import org.junit.jupiter.api.Test;

import org.pipelineframework.connector.CommandInvocation;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandOutcome;
import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.connector.ResolvedConnection;
import org.pipelineframework.connector.graphql.GraphQlMutationOperation;
import org.pipelineframework.connector.graphql.GraphQlMutationRequest;
import org.pipelineframework.connector.graphql.GraphQlOperationConfiguration;
import org.pipelineframework.connector.graphql.GraphQlQueryOperation;
import org.pipelineframework.connector.graphql.GraphQlQueryRequest;
import org.pipelineframework.connector.graphql.GraphQlResponse;
import org.pipelineframework.connector.graphql.GraphQlVariablesJson;

class SmallRyeGraphQlConnectorTest {
    private static final String QUERY_SHA = "bce03f6448ac6810ddd31d019cc6f6ebb9644d1c556d1b67fd64328fe91bdc18";
    private static final String MUTATION_SHA = "d8880449c38b3c50c2102bff7a8132267d31ec249ba6e60d2d721d6c21235514";

    @Test void activatesOfflineAndExecutesTenantResolvedQuery() {
        DynamicGraphQLClient client = mock(DynamicGraphQLClient.class);
        Response response = response("{\"customer\":{\"id\":\"7\",\"name\":\"Ada\"}}", List.of());
        when(client.executeAsync(anyString(), anyMap(), eq("CustomerLookup")))
            .thenReturn(Uni.createFrom().item(response));
        List<ConnectionResolutionRequest<?>> resolutions = new ArrayList<>();
        SmallRyeGraphQlConnector connector = started(client, resolutions);

        assertTrue(resolutions.isEmpty(), "activation must not resolve a connection or contact an endpoint");
        QueryOutcome<GraphQlResponse> outcome = query(connector, "customer.lookup", context("tenant-a"));

        GraphQlResponse output = (GraphQlResponse) assertInstanceOf(QueryOutcome.Found.class, outcome).output();
        assertEquals("{\"customer\":{\"id\":\"7\",\"name\":\"Ada\"}}",
            output.data().orElseThrow().value());
        assertEquals("tenant-a", resolutions.getFirst().invocationContext().tenantId().orElseThrow());
        verify(client).executeAsync(anyString(), anyMap(), eq("CustomerLookup"));
    }

    @Test void preservesValidGraphQlErrorResponsesForQueryAndMutation() {
        DynamicGraphQLClient client = mock(DynamicGraphQLClient.class);
        GraphQLError error = mock(GraphQLError.class);
        when(error.getCode()).thenReturn("BAD_USER_INPUT");
        when(error.getPath()).thenReturn(new Object[]{"updateCustomer", "name"});
        when(error.getMessage()).thenReturn("bad\u0001 name");
        Response response = response(null, List.of(error));
        when(client.executeAsync(anyString(), anyMap(), anyString())).thenReturn(Uni.createFrom().item(response));
        SmallRyeGraphQlConnector connector = started(client, new ArrayList<>());

        GraphQlResponse query = (GraphQlResponse) assertInstanceOf(QueryOutcome.Found.class,
            query(connector, "customer.lookup", context("tenant-a"))).output();
        CommandOutcome.Succeeded<GraphQlResponse> mutation = assertInstanceOf(CommandOutcome.Succeeded.class,
            mutation(connector, "customer.update", context("tenant-a")));

        assertEquals("bad-user-input", query.errors().getFirst().code());
        assertEquals("bad name", query.errors().getFirst().message());
        assertEquals(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED,
            mutation.confirmation().machineConfirmation());
        assertEquals(List.of("updateCustomer", "name"), mutation.output().errors().getFirst().path());
    }

    @Test void rejectsUnknownAndWrongKindOperationsBeforeDispatch() {
        DynamicGraphQLClient client = mock(DynamicGraphQLClient.class);
        SmallRyeGraphQlConnector connector = started(client, new ArrayList<>());

        assertThrows(Exception.class, () -> query(connector, "missing", context("tenant-a")));
        assertThrows(Exception.class, () -> query(connector, "customer.update", context("tenant-a")));
        assertInstanceOf(CommandOutcome.TerminalFailure.class,
            mutation(connector, "customer.lookup", context("tenant-a")));

        verify(client, never()).executeAsync(anyString(), anyMap(), anyString());
    }

    @Test void treatsAnInactiveMutationBindingAsRetryableRatherThanUnauthorized() {
        SmallRyeGraphQlConnector inactive = new SmallRyeGraphQlConnector(getClass().getClassLoader());

        assertInstanceOf(CommandOutcome.RetryableFailure.class,
            mutation(inactive, "customer.update", context("tenant-a")));
    }

    @Test void mapsDefiniteConnectionFailureAndPostDispatchFailureWithoutUnsafeRedrive() {
        DynamicGraphQLClient client = mock(DynamicGraphQLClient.class);
        when(client.executeAsync(anyString(), anyMap(), eq("CustomerUpdate")))
            .thenReturn(Uni.createFrom().failure(new InvalidResponseException("connection closed")));
        SmallRyeGraphQlConnector connector = started(client, new ArrayList<>());

        assertInstanceOf(CommandOutcome.Ambiguous.class,
            mutation(connector, "customer.update", context("tenant-a")));
        var operation = mutationOperation(connector);
        assertTrue(!operation.capabilities().retryRedriveSupported());
        assertTrue(!operation.capabilities().providerIdempotencySupported());
        assertTrue(!operation.capabilities().reconciliationSupported());

        SmallRyeGraphQlConnector unavailable = new SmallRyeGraphQlConnector(getClass().getClassLoader());
        unavailable.start(runtimeContext(failingResolver()), configuration()).toCompletableFuture().join();
        assertInstanceOf(CommandOutcome.RetryableFailure.class,
            mutation(unavailable, "customer.update", context("tenant-b")));
    }

    @Test void mapsSynchronousAndNullConnectionResolutionFailuresToRetryableMutationOutcomes() {
        SmallRyeGraphQlConnector synchronous = new SmallRyeGraphQlConnector(getClass().getClassLoader());
        synchronous.start(runtimeContext(new ConnectionResolver() {
            @Override
            public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
                throw new IllegalStateException("synchronous resolver failure");
            }
        }), configuration()).toCompletableFuture().join();
        assertInstanceOf(CommandOutcome.RetryableFailure.class,
            mutation(synchronous, "customer.update", context("tenant-a")));

        SmallRyeGraphQlConnector nullStage = new SmallRyeGraphQlConnector(getClass().getClassLoader());
        nullStage.start(runtimeContext(new ConnectionResolver() {
            @Override
            public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
                return null;
            }
        }), configuration()).toCompletableFuture().join();
        assertInstanceOf(CommandOutcome.RetryableFailure.class,
            mutation(nullStage, "customer.update", context("tenant-a")));
    }

    @Test void distinguishesSynchronousPreDispatchFailureFromInvalidPostDispatchResponse() {
        DynamicGraphQLClient synchronousFailure = mock(DynamicGraphQLClient.class);
        when(synchronousFailure.executeAsync(anyString(), anyMap(), eq("CustomerUpdate")))
            .thenThrow(new IllegalStateException("request construction failed"));
        assertInstanceOf(CommandOutcome.RetryableFailure.class,
            mutation(started(synchronousFailure, new ArrayList<>()), "customer.update", context("tenant-a")));

        DynamicGraphQLClient invalidResponse = mock(DynamicGraphQLClient.class);
        Response emptyResponse = response(null, List.of());
        when(invalidResponse.executeAsync(anyString(), anyMap(), eq("CustomerUpdate")))
            .thenReturn(Uni.createFrom().item(emptyResponse));
        assertInstanceOf(CommandOutcome.Ambiguous.class,
            mutation(started(invalidResponse, new ArrayList<>()), "customer.update", context("tenant-a")));
    }

    @Test void propagatesQueryConnectionAndTransportFailuresThroughNativeQuerySemantics() {
        SmallRyeGraphQlConnector unavailable = new SmallRyeGraphQlConnector(getClass().getClassLoader());
        unavailable.start(runtimeContext(failingResolver()), configuration()).toCompletableFuture().join();

        CompletionException connectionFailure = assertThrows(CompletionException.class,
            () -> query(unavailable, "customer.lookup", context("tenant-b")));
        assertInstanceOf(IllegalStateException.class, connectionFailure.getCause());

        DynamicGraphQLClient temporaryFailureClient = mock(DynamicGraphQLClient.class);
        when(temporaryFailureClient.executeAsync(anyString(), anyMap(), eq("CustomerLookup")))
            .thenReturn(Uni.createFrom().failure(new InvalidResponseException("temporary provider failure")));
        SmallRyeGraphQlConnector temporaryFailure = started(temporaryFailureClient, new ArrayList<>());

        CompletionException dispatchFailure = assertThrows(CompletionException.class,
            () -> query(temporaryFailure, "customer.lookup", context("tenant-c")));
        assertInstanceOf(InvalidResponseException.class, dispatchFailure.getCause());
        verify(temporaryFailureClient).executeAsync(anyString(), anyMap(), eq("CustomerLookup"));
    }

    @Test void neverClosesTheHostOwnedClient() throws Exception {
        DynamicGraphQLClient client = mock(DynamicGraphQLClient.class);
        SmallRyeGraphQlConnector connector = started(client, new ArrayList<>());

        connector.stop(ConnectorRuntimeContext.empty()).toCompletableFuture().join();

        verify(client, never()).close();
    }

    @Test void validatesDigestNameAndKindWithoutNetworkAccess() {
        DynamicGraphQLClient client = mock(DynamicGraphQLClient.class);
        var connector = new SmallRyeGraphQlConnector(getClass().getClassLoader());

        var badDigest = configuration(Map.of("customer.lookup", new GraphQlPersistedOperation(
            GraphQlOperationKind.QUERY, "CustomerLookup", "graphql/customer-lookup.graphql", "0".repeat(64))));
        CompletionException digestFailure = assertThrows(CompletionException.class,
            () -> connector.start(runtimeContext(resolver(client, new ArrayList<>())), badDigest)
                .toCompletableFuture().join());
        assertInstanceOf(IllegalArgumentException.class, digestFailure.getCause());
        assertTrue(digestFailure.getCause().getMessage().contains("has SHA-256"));

        var badName = configuration(Map.of("customer.lookup", new GraphQlPersistedOperation(
            GraphQlOperationKind.QUERY, "OtherLookup", "graphql/customer-lookup.graphql", QUERY_SHA)));
        CompletionException nameFailure = assertThrows(CompletionException.class,
            () -> connector.start(runtimeContext(resolver(client, new ArrayList<>())), badName)
                .toCompletableFuture().join());
        assertInstanceOf(IllegalArgumentException.class, nameFailure.getCause());
        assertTrue(nameFailure.getCause().getMessage().contains("rather than 'OtherLookup'"));

        var badKind = configuration(Map.of("customer.lookup", new GraphQlPersistedOperation(
            GraphQlOperationKind.MUTATION, "CustomerLookup", "graphql/customer-lookup.graphql", QUERY_SHA)));
        CompletionException kindFailure = assertThrows(CompletionException.class,
            () -> connector.start(runtimeContext(resolver(client, new ArrayList<>())), badKind)
                .toCompletableFuture().join());
        assertInstanceOf(IllegalArgumentException.class, kindFailure.getCause());
        assertTrue(kindFailure.getCause().getMessage().contains("is QUERY but the binding declares MUTATION"));
        verify(client, never()).executeAsync(anyString(), anyMap(), anyString());
    }

    private SmallRyeGraphQlConnector started(
        DynamicGraphQLClient client,
        List<ConnectionResolutionRequest<?>> resolutions
    ) {
        var connector = new SmallRyeGraphQlConnector(getClass().getClassLoader());
        connector.start(runtimeContext(resolver(client, resolutions)), configuration()).toCompletableFuture().join();
        return connector;
    }

    private static GraphQlProviderConfiguration configuration() {
        return configuration(Map.of(
            "customer.lookup", new GraphQlPersistedOperation(
                GraphQlOperationKind.QUERY, "CustomerLookup", "graphql/customer-lookup.graphql", QUERY_SHA),
            "customer.update", new GraphQlPersistedOperation(
                GraphQlOperationKind.MUTATION, "CustomerUpdate", "graphql/customer-update.graphql", MUTATION_SHA)));
    }

    private static GraphQlProviderConfiguration configuration(Map<String, GraphQlPersistedOperation> operations) {
        return new GraphQlProviderConfiguration(new ConnectionRef("primary-graphql"), operations);
    }

    private static ConnectorRuntimeContext runtimeContext(ConnectionResolver resolver) {
        return ConnectorRuntimeContext.of("test", Runnable::run, Clock.systemUTC(), Optional.of(resolver));
    }

    private static ConnectionResolver resolver(
        DynamicGraphQLClient client,
        List<ConnectionResolutionRequest<?>> resolutions
    ) {
        return new ConnectionResolver() {
            @Override
            public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
                resolutions.add(request);
                return CompletableFuture.completedStage(
                    request.connectionType().cast(new AuthenticatedGraphQlConnection(client)));
            }
        };
    }

    private static ConnectionResolver failingResolver() {
        return new ConnectionResolver() {
            @Override
            public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
                return CompletableFuture.failedStage(new IllegalStateException("credential service unavailable"));
            }
        };
    }

    private static ConnectorExecutionContext context(String tenant) {
        return new ConnectorExecutionContext(Optional.of(tenant), Optional.empty(), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static QueryOutcome<GraphQlResponse> query(
        SmallRyeGraphQlConnector connector,
        String operation,
        ConnectorExecutionContext context
    ) {
        var request = new GraphQlQueryRequest(operation, new GraphQlVariablesJson("{\"id\":\"7\"}"));
        return queryOperation(connector).query(new QueryInvocation<>(request, ConnectorConfigurationDocument.empty(),
            GraphQlResponse.class, context)).toCompletableFuture().join();
    }

    private static CommandOutcome<GraphQlResponse> mutation(
        SmallRyeGraphQlConnector connector,
        String operation,
        ConnectorExecutionContext context
    ) {
        var request = new GraphQlMutationRequest(
            operation, "customer-7-update-1", new GraphQlVariablesJson("{\"id\":\"7\",\"name\":\"Ada\"}"));
        return mutationOperation(connector).dispatch(new CommandInvocation<>(request,
            new GraphQlOperationConfiguration(), GraphQlResponse.class, context, Optional.empty()))
            .toCompletableFuture().join();
    }

    private static GraphQlQueryOperation queryOperation(SmallRyeGraphQlConnector connector) {
        return connector.operations().stream()
            .filter(GraphQlQueryOperation.class::isInstance)
            .map(GraphQlQueryOperation.class::cast)
            .findFirst().orElseThrow();
    }

    private static GraphQlMutationOperation mutationOperation(SmallRyeGraphQlConnector connector) {
        return connector.operations().stream()
            .filter(GraphQlMutationOperation.class::isInstance)
            .map(GraphQlMutationOperation.class::cast)
            .findFirst().orElseThrow();
    }

    private static Response response(String dataJson, List<GraphQLError> errors) {
        Response response = mock(Response.class);
        when(response.hasData()).thenReturn(dataJson != null);
        if (dataJson != null) {
            JsonObject data = mock(JsonObject.class);
            when(data.toString()).thenReturn(dataJson);
            when(response.getData()).thenReturn(data);
        }
        when(response.getErrors()).thenReturn(errors);
        return response;
    }
}
