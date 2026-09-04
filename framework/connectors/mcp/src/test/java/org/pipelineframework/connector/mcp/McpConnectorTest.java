package org.pipelineframework.connector.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.modelcontextprotocol.client.McpAsyncClient;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.transport.ServerParameters;
import io.modelcontextprotocol.client.transport.StdioClientTransport;
import io.modelcontextprotocol.json.jackson2.JacksonMcpJsonMapper;
import io.modelcontextprotocol.spec.McpSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.pipelineframework.command.CommandStepSupport;
import org.pipelineframework.connector.ConnectorBindingDefinition;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.CommandInvocation;
import org.pipelineframework.connector.CommandOperation;
import org.pipelineframework.connector.CommandOutcome;
import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.dispatch.BoundOperationReference;
import org.pipelineframework.dispatch.DispatchCapability;
import org.pipelineframework.dispatch.OperationDispatchDescriptor;
import org.pipelineframework.dispatch.OperationDispatchSupport;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.query.InMemoryQueryCaptureStore;
import org.pipelineframework.query.QueryStepSupport;
import org.pipelineframework.connector.QueryCacheability;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOperation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.connector.ResolvedConnection;
import reactor.core.publisher.Mono;

class McpConnectorTest {
    @AfterEach
    void clearExecutionContext() {
        PipelineExecutionContextHolder.clear();
    }

    @Test
    void dispatchesOnlyReleaseExposedMcpOperationThroughOrdinaryAgentCallPath() {
        McpAsyncClient client = initializedClient(new McpSchema.CallToolResult(
            List.of(), false, Map.of("value", "dispatch-found"), Map.of()));
        ConnectorRuntimeContext runtime = ConnectorRuntimeContext.of(
            "test", Runnable::run, Clock.systemUTC(), Optional.of(resolver(client)));
        ConnectorBindingRegistry bindings = ConnectorBindingRegistry.fromProviders(
            List.of(new ConnectorBindingDefinition(
                ConnectorBindingName.of("mcp"), ConnectorProviderId.of("mcp.client"), 1,
                new ConnectorConfigurationDocument(Map.of("connection", "test-mcp")))),
            List.of(new McpConnector()));
        QueryStepSupport queries = new QueryStepSupport(
            List.of(), List.of(new InMemoryQueryCaptureStore()), bindings, runtime);
        OperationDispatchSupport dispatch = new OperationDispatchSupport(
            queries, new CommandStepSupport(), ignored -> {
                throw new AssertionError("Query dispatch must not resolve a command ID generator");
            });
        ConnectorOperationIdentity identity = new ConnectorOperationIdentity(
            ConnectorProviderId.of("mcp.client"), "customer.lookup", ConnectorOperationKind.QUERY, 1);
        DispatchCapability callable = new DispatchCapability(
            new BoundOperationReference(ConnectorBindingName.of("mcp"), "customer.lookup"), identity, 1,
            "McpRequest", McpRequest.class, "McpResult", McpResult.class, Map.of(),
            Optional.of(org.pipelineframework.connector.QueryCapabilities.conservative()), Optional.empty());
        OperationDispatchDescriptor catalogue = OperationDispatchDescriptor.of("Invoke proposal", List.of(callable));
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "mcp-dispatch", 0));

        assertThrows(IllegalArgumentException.class, () -> dispatch.dispatch(
                catalogue, "mcp", "customer.write", "{\"id\":\"42\"}", OperationObservation.class)
            .await().atMost(Duration.ofSeconds(2)));
        verify(client, never()).callTool(any());

        OperationObservation.Result observation = assertInstanceOf(OperationObservation.Result.class,
            dispatch.dispatch(catalogue, "mcp", "customer.lookup", "{\"id\":\"42\"}",
                    OperationObservation.class)
                .await().atMost(Duration.ofSeconds(2)));

        assertEquals("mcp", observation.value().binding());
        assertEquals("customer.lookup", observation.value().operation());
        assertEquals("tpf:query", observation.value().kind());
        assertEquals("found", observation.value().outcome());
        assertEquals("{\"value\":\"dispatch-found\"}", observation.value().resultJson());
        verify(client).callTool(any());
        verify(client, never()).close();
    }

    @Test
    void invokesARealMcpServerOverHostOwnedStdio() {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        ServerParameters parameters = ServerParameters.builder(java)
            .args("-cp", System.getProperty("java.class.path"), McpStdioFixtureMain.class.getName())
            .build();
        StdioClientTransport transport = new StdioClientTransport(
            parameters, new JacksonMcpJsonMapper(new ObjectMapper()));
        McpAsyncClient client = McpClient.async(transport).build();
        try {
            client.initialize().toFuture().join();
            McpConnector connector = started(client);
            QueryOperation<Object, ConnectorConfigurationDocument, McpResult> operation = query(
                connector, "customer.lookup");

            QueryOutcome<McpResult> outcome = operation.query(new QueryInvocation<>(
                new McpRequest("42"), ConnectorConfigurationDocument.empty(), McpResult.class,
                ConnectorExecutionContext.empty())).toCompletableFuture().join();

            QueryOutcome.Found<McpResult> found = assertInstanceOf(QueryOutcome.Found.class, outcome);
            assertEquals(new McpResult("real-42"), found.output());
        } finally {
            client.close();
        }
    }
    @Test
    void invokesPinnedQueryThroughHostOwnedClientAndReturnsTypedCanonicalOutput() {
        McpAsyncClient client = initializedClient(new McpSchema.CallToolResult(
            List.of(), false, Map.of("value", "found"), Map.of()));
        McpConnector connector = started(client);
        QueryOperation<Object, ConnectorConfigurationDocument, McpResult> operation = query(
            connector, "customer.lookup");

        QueryOutcome<McpResult> outcome = operation.query(new QueryInvocation<>(
            new McpRequest("42"), ConnectorConfigurationDocument.empty(), McpResult.class,
            ConnectorExecutionContext.empty())).toCompletableFuture().join();

        QueryOutcome.Found<McpResult> found = assertInstanceOf(QueryOutcome.Found.class, outcome);
        assertEquals(new McpResult("found"), found.output());
        assertEquals(QueryCacheability.LIVE_ONLY, operation.capabilities().cacheability());
        verify(client).callTool(any());
        verify(client, never()).close();
    }

    @Test
    void classifiesMcpErrorWithoutRequiringStructuredContent() {
        McpAsyncClient client = initializedClient(new McpSchema.CallToolResult(
            List.of(), true, null, Map.of()));
        QueryOperation<Object, ConnectorConfigurationDocument, McpResult> operation = query(
            started(client), "customer.lookup");

        QueryOutcome<McpResult> outcome = operation.query(new QueryInvocation<>(
            new McpRequest("42"), ConnectorConfigurationDocument.empty(), McpResult.class,
            ConnectorExecutionContext.empty())).toCompletableFuture().join();

        QueryOutcome.TerminalFailure<McpResult> failure = assertInstanceOf(
            QueryOutcome.TerminalFailure.class, outcome);
        assertEquals("mcp-invalid-result", failure.code());
    }

    @Test
    void mapsPostDispatchCommandFailureToAmbiguousWithoutClosingHostClient() {
        McpAsyncClient client = mock(McpAsyncClient.class);
        when(client.isInitialized()).thenReturn(true);
        when(client.callTool(any())).thenReturn(Mono.error(new IllegalStateException("secret provider body")));
        McpConnector connector = started(client);
        CommandOperation<Object, ConnectorConfigurationDocument, Object> operation = command(connector, "customer.write");

        CommandOutcome<Object> outcome = operation.dispatch(new CommandInvocation<>(
            new McpRequest("42"), ConnectorConfigurationDocument.empty(), McpResult.class,
            ConnectorExecutionContext.empty(), Optional.empty())).toCompletableFuture().join();

        CommandOutcome.Ambiguous<Object> ambiguous = assertInstanceOf(CommandOutcome.Ambiguous.class, outcome);
        assertEquals("mcp-dispatch-uncertain", ambiguous.code());
        verify(client, never()).close();
    }

    private static McpConnector started(McpAsyncClient client) {
        McpConnector connector = new McpConnector();
        connector.start(
            ConnectorRuntimeContext.of("test", Runnable::run, Clock.systemUTC(), Optional.of(resolver(client))),
            new McpProviderConfiguration(new ConnectionRef("test-mcp")))
            .toCompletableFuture().join();
        return connector;
    }

    private static McpAsyncClient initializedClient(McpSchema.CallToolResult result) {
        McpAsyncClient client = mock(McpAsyncClient.class);
        when(client.isInitialized()).thenReturn(true);
        when(client.callTool(any())).thenReturn(Mono.just(result));
        return client;
    }

    private static ConnectionResolver resolver(McpAsyncClient client) {
        return new ConnectionResolver() {
            @Override
            public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
                assertEquals("test-mcp", request.reference().value());
                assertSame(McpClientConnection.class, request.connectionType());
                return CompletableFuture.completedStage(request.connectionType().cast(new McpClientConnection(client)));
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static QueryOperation<Object, ConnectorConfigurationDocument, McpResult> query(
        McpConnector connector,
        String id
    ) {
        return (QueryOperation<Object, ConnectorConfigurationDocument, McpResult>) (QueryOperation<?, ?, ?>)
            connector.operations().stream()
            .filter(operation -> operation.id().equals(id)).findFirst().orElseThrow();
    }

    @SuppressWarnings("unchecked")
    private static CommandOperation<Object, ConnectorConfigurationDocument, Object> command(
        McpConnector connector,
        String id
    ) {
        return (CommandOperation<Object, ConnectorConfigurationDocument, Object>) connector.operations().stream()
            .filter(operation -> operation.id().equals(id)).findFirst().orElseThrow();
    }

    record McpRequest(String id) {
    }

    record McpResult(String value) {
    }
}
