package org.pipelineframework.connector.gmail;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.gmail.Gmail;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorBindingDefinition;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorInvocationTarget;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOperation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.connector.ResolvedConnection;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.query.InMemoryQueryCaptureStore;
import org.pipelineframework.query.NativeQuerySelector;
import org.pipelineframework.query.QueryStepDescriptor;
import org.pipelineframework.query.QueryStepSupport;

class GmailQueryConnectorTest {
    private static final ConnectorBindingName BINDING = ConnectorBindingName.of("gmail-primary");

    @AfterEach
    void clearExecutionContext() {
        PipelineExecutionContextHolder.clear();
    }

    @Test
    void resolvesTenantSpecificClientAndReplaysCapturedResultWithoutResolution() {
        AtomicInteger resolutions = new AtomicInteger();
        List<ConnectionResolutionRequest<?>> requests = new ArrayList<>();
        Gmail tenantA = client(url -> jsonResponse(200, """
            {"messages":[{"id":"message-a","threadId":"thread-a"}],
             "nextPageToken":"next-a","resultSizeEstimate":1}
            """));
        Gmail tenantB = client(url -> jsonResponse(200, """
            {"messages":[{"id":"message-b","threadId":"thread-b"}],
             "resultSizeEstimate":1}
            """));
        ConnectionResolver resolver = resolver(Map.of("tenant-a", tenantA, "tenant-b", tenantB), requests, resolutions);
        ConnectorRuntimeContext runtimeContext = runtimeContext(resolver);
        InMemoryQueryCaptureStore captureStore = new InMemoryQueryCaptureStore();
        ConnectorBindingDefinition definition = bindingDefinition();
        ConnectorBindingRegistry bindings = ConnectorBindingRegistry.fromProviders(
            List.of(definition), List.of(new GmailQueryConnector()));
        QueryStepDescriptor descriptor = descriptor(
            "list.messages", GmailListMessagesRequest.class, GmailMessagePage.class,
            Map.of("maxResults", 25L, "includeSpamTrash", false));
        QueryStepSupport support = new QueryStepSupport(List.of(), List.of(captureStore), bindings, runtimeContext);
        PipelineExecutionContextHolder.set(execution("tenant-a"));

        GmailMessagePage first = support.queryOneToOne(
            descriptor, new GmailListMessagesRequest(Optional.empty()), GmailMessagePage.class)
            .await().atMost(Duration.ofSeconds(2));

        ConnectorBindingRegistry unavailable = ConnectorBindingRegistry.fromProvidersAllowingUnavailable(
            List.of(definition), List.of());
        QueryStepSupport replay = new QueryStepSupport(
            List.of(), List.of(captureStore), unavailable, ConnectorRuntimeContext.empty());
        GmailMessagePage replayed = replay.queryOneToOne(
            descriptor, new GmailListMessagesRequest(Optional.empty()), GmailMessagePage.class)
            .await().atMost(Duration.ofSeconds(2));

        assertEquals("message-a", first.messages().getFirst().id());
        assertEquals(first, replayed);
        assertEquals(1, resolutions.get());
        assertEquals(new org.pipelineframework.connector.ConnectionRef("gmail.primary"),
            requests.getFirst().reference());
        assertEquals(Optional.of("tenant-a"), requests.getFirst().invocationContext().tenantId());
        assertEquals(Optional.of("release-9"), requests.getFirst().invocationContext().releaseVersion());
        assertEquals(GmailQueryConnector.PROVIDER_ID,
            requests.getFirst().invocationContext().invocationTarget().orElseThrow().operation().providerId());
    }

    @Test
    void listAndSearchUseOnlyInvocationSemanticsWhileTenantSelectsTheClient() {
        AtomicReference<String> tenantAUrl = new AtomicReference<>();
        AtomicReference<String> tenantBUrl = new AtomicReference<>();
        Gmail tenantA = client(url -> {
            tenantAUrl.set(url);
            return jsonResponse(200,
                "{\"messages\":[{\"id\":\"message-a\",\"threadId\":\"thread-a\"}],\"resultSizeEstimate\":1}");
        });
        Gmail tenantB = client(url -> {
            tenantBUrl.set(url);
            return jsonResponse(200,
                "{\"messages\":[{\"id\":\"message-b\",\"threadId\":\"thread-b\"}],\"resultSizeEstimate\":1}");
        });
        List<ConnectionResolutionRequest<?>> requests = new ArrayList<>();
        GmailQueryConnector connector = startedConnector(
            resolver(Map.of("tenant-a", tenantA, "tenant-b", tenantB), requests, new AtomicInteger()));

        QueryOperation<GmailListMessagesRequest, GmailListMessagesConfiguration, GmailMessagePage> listOperation =
            operation(connector, "list.messages");
        QueryOperation<GmailSearchMessagesRequest, GmailListMessagesConfiguration, GmailMessagePage> searchOperation =
            operation(connector, "search.messages");
        QueryOutcome<GmailMessagePage> listed = listOperation.query(new QueryInvocation<>(
            new GmailListMessagesRequest(Optional.of("page-a")),
            new GmailListMessagesConfiguration(Optional.of(12L), true),
            GmailMessagePage.class,
            context("tenant-a", "list.messages"))).toCompletableFuture().join();
        QueryOutcome<GmailMessagePage> searched = searchOperation.query(new QueryInvocation<>(
            new GmailSearchMessagesRequest("from:alice@example.com", Optional.empty()),
            new GmailListMessagesConfiguration(Optional.of(7L), false),
            GmailMessagePage.class,
            context("tenant-b", "search.messages"))).toCompletableFuture().join();

        GmailMessagePage listedPage = (GmailMessagePage) assertInstanceOf(QueryOutcome.Found.class, listed).output();
        GmailMessagePage searchedPage = (GmailMessagePage) assertInstanceOf(QueryOutcome.Found.class, searched).output();
        assertEquals("message-a", listedPage.messages().getFirst().id());
        assertEquals("message-b", searchedPage.messages().getFirst().id());
        assertTrue(tenantAUrl.get().contains("maxResults=12"));
        assertTrue(tenantAUrl.get().contains("includeSpamTrash=true"));
        assertTrue(tenantAUrl.get().contains("pageToken=page-a"));
        assertTrue(tenantBUrl.get().contains("maxResults=7"));
        assertTrue(java.net.URLDecoder.decode(tenantBUrl.get(), java.nio.charset.StandardCharsets.UTF_8)
            .contains("q=from:alice@example.com"));
        assertEquals(List.of("tenant-a", "tenant-b"), requests.stream()
            .map(request -> request.invocationContext().tenantId().orElseThrow()).toList());
        assertEquals(List.of("pageToken"), componentNames(GmailListMessagesRequest.class));
        assertEquals(List.of("query", "pageToken"), componentNames(GmailSearchMessagesRequest.class));
    }

    @Test
    void getProjectsMessageWithoutExposingTheGoogleSdkModel() {
        Gmail client = client(url -> jsonResponse(200, """
            {"id":"message-1","threadId":"thread-1","labelIds":["INBOX"],
             "snippet":"hello","internalDate":"1725000000000",
             "payload":{"headers":[{"name":"Subject","value":"Status"}],
                        "body":{"data":"SGVsbG8"}}}
            """));
        GmailQueryConnector connector = startedConnector(
            resolver(Map.of("tenant-a", client), new ArrayList<>(), new AtomicInteger()));

        QueryOperation<GmailGetMessageRequest, GmailGetMessageConfiguration, GmailMessage> getOperation =
            operation(connector, "get.message");
        QueryOutcome<GmailMessage> outcome = getOperation.query(new QueryInvocation<>(
            new GmailGetMessageRequest("message-1"),
            new GmailGetMessageConfiguration(),
            GmailMessage.class,
            context("tenant-a", "get.message"))).toCompletableFuture().join();

        GmailMessage message = (GmailMessage) assertInstanceOf(QueryOutcome.Found.class, outcome).output();
        assertEquals("message-1", message.id());
        assertEquals(List.of("INBOX"), message.labelIds());
        assertEquals(Optional.of("Status"), message.headers().stream()
            .filter(header -> header.name().equals("Subject"))
            .map(GmailMessageHeader::value)
            .findFirst());
        assertEquals(Optional.of("SGVsbG8"), message.bodyData());
        assertFalse(message.getClass().getName().startsWith("com.google"));
    }

    @Test
    void missingTenantAndResolverFailuresReturnCredentialFreeAuthenticationOutcome() {
        AtomicInteger resolutions = new AtomicInteger();
        ConnectionResolver resolver = new ConnectionResolver() {
            @Override
            public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
                resolutions.incrementAndGet();
                return CompletableFuture.failedStage(new org.pipelineframework.connector.ConnectionResolutionException(
                    "refresh-token=do-not-leak"));
            }
        };
        GmailQueryConnector connector = startedConnector(resolver);
        QueryOperation<GmailListMessagesRequest, GmailListMessagesConfiguration, GmailMessagePage> operation =
            operation(connector, "list.messages");

        QueryOutcome<GmailMessagePage> missingTenant = operation.query(new QueryInvocation<>(
            new GmailListMessagesRequest(Optional.empty()),
            new GmailListMessagesConfiguration(Optional.empty(), false),
            GmailMessagePage.class,
            ConnectorExecutionContext.empty())).toCompletableFuture().join();
        QueryOutcome<GmailMessagePage> failedResolution = operation.query(new QueryInvocation<>(
            new GmailListMessagesRequest(Optional.empty()),
            new GmailListMessagesConfiguration(Optional.empty(), false),
            GmailMessagePage.class,
            context("tenant-a", "list.messages"))).toCompletableFuture().join();

        assertEquals("gmail-authentication-required", missingTenant.code());
        assertEquals("gmail-authentication-required", failedResolution.code());
        assertFalse(failedResolution.toString().contains("do-not-leak"));
        assertEquals(1, resolutions.get());
        assertEquals("https://www.googleapis.com/auth/gmail.readonly", GmailQueryConnector.REQUIRED_OAUTH_SCOPE);
    }

    @Test
    void liveRetryResolvesAgainWithTheSamePinnedInvocationContext() {
        AtomicInteger calls = new AtomicInteger();
        Gmail client = client(url -> calls.getAndIncrement() == 0
            ? jsonResponse(503, "{\"error\":{\"code\":503,\"message\":\"unavailable\"}}")
            : jsonResponse(200, "{\"messages\":[],\"resultSizeEstimate\":0}"));
        List<ConnectionResolutionRequest<?>> requests = new ArrayList<>();
        AtomicInteger resolutions = new AtomicInteger();
        GmailQueryConnector connector = startedConnector(
            resolver(Map.of("tenant-a", client), requests, resolutions));
        QueryOperation<GmailListMessagesRequest, GmailListMessagesConfiguration, GmailMessagePage> operation =
            operation(connector, "list.messages");
        ConnectorExecutionContext invocationContext = context("tenant-a", "list.messages");
        QueryInvocation<GmailListMessagesRequest, GmailListMessagesConfiguration, GmailMessagePage> invocation =
            new QueryInvocation<>(
                new GmailListMessagesRequest(Optional.empty()),
                new GmailListMessagesConfiguration(Optional.of(10L), false),
                GmailMessagePage.class,
                invocationContext);

        QueryOutcome<GmailMessagePage> first = operation.query(invocation).toCompletableFuture().join();
        QueryOutcome<GmailMessagePage> retried = operation.query(invocation).toCompletableFuture().join();

        assertInstanceOf(QueryOutcome.TemporarilyUnavailable.class, first);
        assertInstanceOf(QueryOutcome.Found.class, retried);
        assertEquals(2, resolutions.get());
        assertEquals(List.of(invocationContext, invocationContext), requests.stream()
            .map(ConnectionResolutionRequest::invocationContext)
            .toList());
    }

    @Test
    void permanentGmailClientErrorsAreTerminal() {
        Gmail client = client(url -> jsonResponse(400, "{\"error\":{\"code\":400,\"message\":\"bad query\"}}"));
        GmailQueryConnector connector = startedConnector(
            resolver(Map.of("tenant-a", client), new ArrayList<>(), new AtomicInteger()));
        QueryOperation<GmailSearchMessagesRequest, GmailListMessagesConfiguration, GmailMessagePage> operation =
            operation(connector, "search.messages");

        QueryOutcome<GmailMessagePage> outcome = operation.query(new QueryInvocation<>(
            new GmailSearchMessagesRequest("invalid-query", Optional.empty()),
            new GmailListMessagesConfiguration(Optional.empty(), false),
            GmailMessagePage.class,
            context("tenant-a", "search.messages"))).toCompletableFuture().join();

        assertInstanceOf(QueryOutcome.TerminalFailure.class, outcome);
        assertEquals("gmail-query-failed", outcome.code());
    }

    @Test
    void invocationRecordsRejectUnicodeSurroundingWhitespace() {
        assertThrows(IllegalArgumentException.class,
            () -> new GmailListMessagesRequest(Optional.of("\u2003page-token")));
        assertThrows(IllegalArgumentException.class, () -> new GmailGetMessageRequest("message-id\u2003"));
        assertThrows(IllegalArgumentException.class,
            () -> new GmailSearchMessagesRequest("\u2003from:alice@example.com", Optional.empty()));
    }

    @Test
    void providerDescriptorExposesOnlyTheThreeReadQueryOperations() {
        GmailQueryConnector connector = new GmailQueryConnector();

        assertEquals(GmailQueryConnector.PROVIDER_ID, connector.id());
        assertEquals(List.of("get.message", "list.messages", "search.messages"), connector.operations().stream()
            .map(operation -> operation.id())
            .sorted()
            .toList());
        assertTrue(connector.operations().stream().allMatch(QueryOperation.class::isInstance));
    }

    private static GmailQueryConnector startedConnector(ConnectionResolver resolver) {
        GmailQueryConnector connector = new GmailQueryConnector();
        connector.start(runtimeContext(resolver),
            new GmailProviderConfiguration(new org.pipelineframework.connector.ConnectionRef("gmail.primary")))
            .toCompletableFuture().join();
        return connector;
    }

    private static ConnectorRuntimeContext runtimeContext(ConnectionResolver resolver) {
        return ConnectorRuntimeContext.of(
            "test", Runnable::run, Clock.systemUTC(), Optional.of(resolver), Optional.empty());
    }

    private static ConnectionResolver resolver(
        Map<String, Gmail> clients,
        List<ConnectionResolutionRequest<?>> requests,
        AtomicInteger resolutions
    ) {
        return new ConnectionResolver() {
            @Override
            public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
                requests.add(request);
                resolutions.incrementAndGet();
                String tenant = request.invocationContext().tenantId().orElseThrow();
                return CompletableFuture.completedStage(request.connectionType().cast(
                    new AuthenticatedGmailConnection(clients.get(tenant))));
            }
        };
    }

    private static ConnectorExecutionContext context(String tenantId, String operationId) {
        ConnectorOperationIdentity identity = new ConnectorOperationIdentity(
            GmailQueryConnector.PROVIDER_ID, operationId, ConnectorOperationKind.QUERY, 1);
        return ConnectorExecutionContext.managed(
            tenantId,
            "execution-17",
            "mail-pipeline",
            "contract-3",
            "release-9",
            "ReadInbox",
            new ConnectorInvocationTarget(BINDING, identity),
            Optional.of("correlation-5"),
            Optional.of("trace-7"),
            Optional.empty());
    }

    private static PipelineExecutionContext execution(String tenantId) {
        return new PipelineExecutionContext(
            tenantId, "execution-17", "mail-pipeline", "contract-3", "release-9", 0,
            Optional.of("correlation-5"), Optional.of("trace-7"));
    }

    private static ConnectorBindingDefinition bindingDefinition() {
        return new ConnectorBindingDefinition(
            BINDING,
            GmailQueryConnector.PROVIDER_ID,
            1,
            new ConnectorConfigurationDocument(Map.of("connection", "gmail.primary")));
    }

    private static QueryStepDescriptor descriptor(
        String operationId,
        Class<?> inputType,
        Class<?> outputType,
        Map<String, Object> configuration
    ) {
        ConnectorOperationIdentity identity = new ConnectorOperationIdentity(
            GmailQueryConnector.PROVIDER_ID, operationId, ConnectorOperationKind.QUERY, 1);
        return QueryStepDescriptor.nativeQuery(
            "ReadInbox",
            inputType.getName(),
            outputType.getName(),
            "ONE_TO_ONE",
            new NativeQuerySelector(BINDING, identity, 1),
            configuration,
            QueryCapabilities.cacheable(),
            Optional.empty());
    }

    @SuppressWarnings("unchecked")
    private static <I, C, O> QueryOperation<I, C, O> operation(GmailQueryConnector connector, String id) {
        return (QueryOperation<I, C, O>) connector.operations().stream()
            .filter(operation -> operation.id().equals(id))
            .findFirst()
            .orElseThrow();
    }

    private static List<String> componentNames(Class<?> recordType) {
        return java.util.Arrays.stream(recordType.getRecordComponents())
            .map(java.lang.reflect.RecordComponent::getName)
            .toList();
    }

    private static Gmail client(Function<String, LowLevelHttpResponse> response) {
        MockHttpTransport transport = new MockHttpTransport() {
            @Override
            public LowLevelHttpRequest buildRequest(String method, String url) throws IOException {
                return new MockLowLevelHttpRequest(url) {
                    @Override
                    public LowLevelHttpResponse execute() {
                        return response.apply(url);
                    }
                };
            }
        };
        return new Gmail.Builder(transport, GsonFactory.getDefaultInstance(), request -> { })
            .setApplicationName("TPF Gmail connector test")
            .build();
    }

    private static MockLowLevelHttpResponse jsonResponse(int statusCode, String json) {
        return new MockLowLevelHttpResponse()
            .setStatusCode(statusCode)
            .setContentType("application/json")
            .setContent(json);
    }
}
