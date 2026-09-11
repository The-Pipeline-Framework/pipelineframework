package org.pipelineframework.connector.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.CommandCapabilities;
import org.pipelineframework.connector.CommandDispatchIdentity;
import org.pipelineframework.connector.CommandExecutionPosture;
import org.pipelineframework.connector.CommandInvocation;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandOperation;
import org.pipelineframework.connector.CommandOutcome;
import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorOperationDescriptor;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorOperationTypeContract;
import org.pipelineframework.connector.ConnectorProviderArtifactDescriptor;
import org.pipelineframework.connector.ConnectorProviderDescriptor;
import org.pipelineframework.connector.ConnectorProviderManifest;
import org.pipelineframework.connector.ConnectorProviderManifestCatalog;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOperation;
import org.pipelineframework.connector.QueryOperationCardinality;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.connector.ResolvedConnection;

class HttpConnectorTest {
    private static final String SOURCE = "1".repeat(64);
    private static final String DIRECT = "2".repeat(64);
    private static final HttpWireSchema STRING = new HttpWireSchema("{\"type\":\"string\"}");
    private static final HttpWireSchema INTEGER = new HttpWireSchema("{\"type\":\"integer\"}");
    private static final HttpWireSchema OBJECT = new HttpWireSchema("{\"type\":\"object\"}");

    private HttpServer server;
    private URI baseUri;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.start();
        baseUri = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/api");
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void requiresPinnedCallbackAuthorityBeforeIoAndInjectsItAfterMapping() throws Exception {
        AtomicReference<String> seen = new AtomicReference<>("");
        server.createContext("/api/jobs", exchange -> {
            seen.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            respond(exchange, 200, "application/json", "{\"value\":\"accepted\"}");
        });
        var original = commandPin("/jobs");
        var requestSchema = new HttpWireSchema("""
            {"type":"object","properties":{"value":{"type":"string"},"callbackUrl":{"type":"string","format":"uri"}},
             "required":["value","callbackUrl"],"additionalProperties":false}
            """);
        var callback = new HttpCallbackPin("job.completed", original.operation(), 1,
            new HttpCallbackInjectionTarget(HttpCallbackInjectionTarget.Location.BODY, List.of("callbackUrl"), Optional.empty()),
            "POST", "application/json", OBJECT, "http.job.callback", LookupOutput.class.getName(),
            HttpSecurityConstraint.none(), 202, true, SOURCE);
        var pin = new HttpOperationPin(original.operation(), original.kind(), 1, original.inputType(), original.outputType(),
            "POST", "/jobs", List.of(), Optional.of(new HttpRequestBodyPin("application/json", Optional.empty(), true, requestSchema)),
            original.responses(), original.security(), requestSchema, original.requestMappingKey(),
            original.providerIdempotencyKey(), List.of(callback), SOURCE);
        var connector = connector(pin);
        connector.start(runtime(connection(Set.of(), HttpAuthorizationProvider.none())), configuration()).toCompletableFuture().join();
        var operation = command(connector);
        assertEquals(List.of(callback.descriptor()), operation.callbacks());
        var identity = Optional.of(new CommandDispatchIdentity("command", "occurrence", "attempt"));
        var context = org.pipelineframework.connector.ConnectorExecutionContext.empty();
        for (var invalid : List.<Optional<org.pipelineframework.connector.ConnectorCallbackContext>>of(Optional.empty(),
            Optional.of(new org.pipelineframework.connector.ConnectorCallbackContext("wrong", URI.create("https://app.test/callback"))))) {
            var result = operation.dispatch(new CommandInvocation<>(new RecordInput("new"), ConnectorConfigurationDocument.empty(),
                LookupOutput.class, context, identity, invalid)).toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertInstanceOf(CommandOutcome.TerminalFailure.class, result);
            assertEquals("", seen.get());
        }
        var authority = new org.pipelineframework.connector.ConnectorCallbackContext("job.completed",
            URI.create("https://app.test/callback?token=signed-token"));
        var plaintext = operation.dispatch(new CommandInvocation<>(new RecordInput("new"), ConnectorConfigurationDocument.empty(),
            LookupOutput.class, context, identity, Optional.of(authority))).toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertInstanceOf(CommandOutcome.TerminalFailure.class, plaintext);
        assertEquals("", seen.get(), "default callback authority must not be sent to a plaintext provider");
        AtomicReference<HttpRequest> secureRequest = new AtomicReference<>();
        var secureConnector = connector(pin);
        secureConnector.start(runtime(connection(successfulClient(secureRequest, "{\"value\":\"accepted\"}"),
            URI.create("https://provider.test/api"), Set.of(), HttpAuthorizationProvider.none())), configuration())
            .toCompletableFuture().join();
        assertInstanceOf(CommandOutcome.Succeeded.class, command(secureConnector).dispatch(new CommandInvocation<>(
            new RecordInput("new"), ConnectorConfigurationDocument.empty(), LookupOutput.class, context, identity,
            Optional.of(authority))).toCompletableFuture().get(5, TimeUnit.SECONDS));
        assertEquals("https", secureRequest.get().uri().getScheme());
        var localAuthority = new org.pipelineframework.connector.ConnectorCallbackContext("job.completed", authority.callbackUri(),
            org.pipelineframework.connector.ConnectorCallbackContext.UriPolicy.LOCAL_HTTP);
        var result = operation.dispatch(new CommandInvocation<>(new RecordInput("new"), ConnectorConfigurationDocument.empty(),
            LookupOutput.class, context, identity, Optional.of(localAuthority))).toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertInstanceOf(CommandOutcome.Succeeded.class, result);
        assertEquals(authority.callbackUri().toString(), org.pipelineframework.config.pipeline.PipelineJson.mapper()
            .readTree(seen.get()).path("callbackUrl").asText());
        var ordinary = connector(original);
        assertInstanceOf(CommandOutcome.TerminalFailure.class, command(ordinary).dispatch(new CommandInvocation<>(
            new RecordInput("new"), ConnectorConfigurationDocument.empty(), LookupOutput.class, context, identity,
            Optional.of(authority))).toCompletableFuture().get(5, TimeUnit.SECONDS));
    }

    @Test
    void executesPinnedPostQueryWithAllParameterLocationsAndHostAuthorization() throws Exception {
        AtomicReference<HttpRequest> seen = new AtomicReference<>();
        HttpClient client = successfulClient(seen, "{\"value\":\"found\"}");
        HttpOperationPin pin = queryPin();
        HttpConnector connector = connector(pin);
        connector.start(runtime(connection(client, URI.create("https://api.example.test/api"), Set.of("oauth2"),
            request -> CompletableFuture.completedStage(
            new HttpAuthorizationMaterial(MapBuilder.headers("Authorization", "Bearer token"),
                MapBuilder.values("api_key", "secret"), java.util.Map.of())))), configuration())
            .toCompletableFuture().join();

        QueryOutcome<LookupOutput> outcome = query(connector).query(new QueryInvocation<>(
            new LookupInput("acme corp", 2, "trace-1", "session-1", new LookupBody("needle")),
            ConnectorConfigurationDocument.empty(), LookupOutput.class,
            org.pipelineframework.connector.ConnectorExecutionContext.empty())).toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertEquals(new LookupOutput("found"), assertInstanceOf(QueryOutcome.Found.class, outcome).output());
        assertEquals("POST", seen.get().method());
        assertEquals("https://api.example.test/api/evidence/acme%20corp/lookup?limit=2&api_key=secret",
            seen.get().uri().toASCIIString());
        assertEquals(Optional.of("trace-1"), seen.get().headers().firstValue("X-Trace"));
        assertEquals(Optional.of("Bearer token"), seen.get().headers().firstValue("Authorization"));
        assertEquals(Optional.of("session=session-1"), seen.get().headers().firstValue("Cookie"));
        assertTrue(seen.get().bodyPublisher().isPresent());
    }

    @Test
    void rejectsAuthorizationFieldsOutsideTheSelectedConstraintWithoutDispatch() throws Exception {
        HttpClient client = mock(HttpClient.class);
        when(client.followRedirects()).thenReturn(HttpClient.Redirect.NEVER);
        HttpConnector connector = connector(queryPin());
        connector.start(runtime(connection(client, URI.create("https://api.example.test/api"), Set.of("oauth2"),
            request -> CompletableFuture.completedStage(
            new HttpAuthorizationMaterial(java.util.Map.of(), MapBuilder.values("limit", "99"), java.util.Map.of())))),
            configuration()).toCompletableFuture().join();

        QueryOutcome<LookupOutput> outcome = query(connector).query(new QueryInvocation<>(
            new LookupInput("acme corp", 2, "trace-1", "session-1", new LookupBody("needle")),
            ConnectorConfigurationDocument.empty(), LookupOutput.class,
            org.pipelineframework.connector.ConnectorExecutionContext.empty())).toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertEquals("http-authorization-failed", outcome.code());
        verify(client, never()).sendAsync(any(), any());
    }

    @Test
    void reservesTheCookieHeaderForCookieLocationAuthorization() {
        assertThrows(IllegalArgumentException.class,
            () -> new HttpAuthorizationTarget(HttpParameterLocation.HEADER, "Cookie"));
        assertThrows(IllegalArgumentException.class, () -> new HttpAuthorizationMaterial(
            MapBuilder.headers("cookie", "host-session=secret"), Map.of(), Map.of()));
        assertEquals(HttpParameterLocation.COOKIE,
            new HttpAuthorizationTarget(HttpParameterLocation.COOKIE, "host-session").location());
    }

    @Test
    void injectsOnlyExplicitProviderIdempotencyAndReturnsAcknowledgedSuccess() throws Exception {
        AtomicReference<String> idempotency = new AtomicReference<>();
        server.createContext("/api/evidence", exchange -> {
            idempotency.set(exchange.getRequestHeaders().getFirst("Idempotency-Key"));
            respond(exchange, 200, "application/json", "{\"value\":\"recorded\"}");
        });
        HttpOperationPin pin = commandPin("/evidence");
        HttpConnector connector = connector(pin);
        connector.start(runtime(connection(Set.of(), HttpAuthorizationProvider.none())), configuration())
            .toCompletableFuture().join();

        CommandOutcome<LookupOutput> outcome = command(connector).dispatch(new CommandInvocation<>(
            new RecordInput("new"), ConnectorConfigurationDocument.empty(), LookupOutput.class,
            org.pipelineframework.connector.ConnectorExecutionContext.empty(),
            Optional.of(new CommandDispatchIdentity("command-1", "occurrence-1", "attempt-1"))))
            .toCompletableFuture().get(5, TimeUnit.SECONDS);

        CommandOutcome.Succeeded<?> succeeded = assertInstanceOf(CommandOutcome.Succeeded.class, outcome);
        assertEquals(new LookupOutput("recorded"), succeeded.output());
        assertEquals(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED,
            succeeded.confirmation().machineConfirmation());
        assertEquals("occurrence-1", idempotency.get());
        assertTrue(command(connector).capabilities().providerIdempotencySupported());
    }

    @Test
    void treatsPostDispatchDisconnectAndUnclassifiedStatusAsAmbiguous() throws Exception {
        server.createContext("/api/drop", HttpExchange::close);
        server.createContext("/api/unclassified", exchange -> respond(exchange, 418, "application/json", "{}"));

        assertEquals("http-dispatch-ambiguous", dispatch(commandPin("/drop")).code());
        assertEquals("http-response-ambiguous", dispatch(commandPin("/unclassified")).code());
    }

    @Test
    void validatesStructuredFailureResponsesBeforeAcceptingTheirOutcome() throws Exception {
        AtomicReference<String> body = new AtomicReference<>("{\"code\":\"conflict\"}");
        server.createContext("/api/structured-failure", exchange ->
            respond(exchange, 409, "application/json", body.get()));
        HttpOperationPin pin = commandPinWithStructuredFailure("/structured-failure");

        assertEquals("evidence-conflict", dispatch(pin).code());

        body.set("{\"message\":\"missing required code\"}");
        assertEquals("http-response-ambiguous", dispatch(pin).code());
    }

    @Test
    void prefersAnExactMediaResponseOverAMediaAgnosticResponse() throws Exception {
        server.createContext("/api/evidence/", exchange ->
            respond(exchange, 200, "application/json", "{\"value\":\"found\"}"));
        HttpOperationPin pin = queryPinWithResponseSpecificity();
        HttpConnector connector = connector(pin);
        connector.start(runtime(connection(Set.of(), HttpAuthorizationProvider.none())), configuration())
            .toCompletableFuture().join();

        QueryOutcome<LookupOutput> outcome = query(connector).query(new QueryInvocation<>(
            new LookupInput("acme corp", 2, "trace-1", "session-1", new LookupBody("needle")),
            ConnectorConfigurationDocument.empty(), LookupOutput.class,
            org.pipelineframework.connector.ConnectorExecutionContext.empty())).toCompletableFuture()
            .get(5, TimeUnit.SECONDS);

        assertEquals(new LookupOutput("found"), assertInstanceOf(QueryOutcome.Found.class, outcome).output());
    }

    @Test
    void rejectsManifestAndPinContractDisagreementAtStartup() {
        HttpOperationPin pin = queryPin();
        ConnectorProviderManifestCatalog wrong = manifests(pin, "WrongOutput");

        IllegalStateException failure = assertThrows(IllegalStateException.class, () -> new HttpConnector(
            new HttpOperationCatalog(List.of(pin)), directBindings(pin), wrong, getClass().getClassLoader()));

        assertTrue(failure.getMessage().contains("type contract disagrees"));
    }

    @Test
    void boundsResponseBodiesDuringStreaming() throws Exception {
        server.createContext("/api/large", exchange ->
            respond(exchange, 200, "application/json", "{\"value\":\"" + "x".repeat(128) + "\"}"));
        HttpConnector connector = connector(commandPin("/large"));
        ConnectorRuntimeContext runtime = runtime(connection(Set.of(), HttpAuthorizationProvider.none()));
        connector.start(runtime, new HttpProviderConfiguration(new ConnectionRef("proof-http"),
            Optional.empty(), Optional.of(16), Optional.empty())).toCompletableFuture().join();

        CommandOutcome<LookupOutput> outcome = command(connector).dispatch(new CommandInvocation<>(
            new RecordInput("new"), ConnectorConfigurationDocument.empty(), LookupOutput.class,
            org.pipelineframework.connector.ConnectorExecutionContext.empty(),
            Optional.of(new CommandDispatchIdentity("command", "occurrence", "attempt"))))
            .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertEquals("http-dispatch-ambiguous", outcome.code());
    }

    @Test
    void requiresRedirectFreeBorrowedClientsAndNeverOwnsTheirLifecycle() {
        assertThrows(IllegalArgumentException.class, () -> new HttpClientConnection(
            HttpClient.newBuilder().followRedirects(HttpClient.Redirect.ALWAYS).build(), baseUri, Set.of(),
            HttpAuthorizationProvider.none()));
        HttpClient borrowed = mock(HttpClient.class);
        when(borrowed.followRedirects()).thenReturn(HttpClient.Redirect.NEVER);
        HttpClientConnection connection = new HttpClientConnection(
            borrowed, baseUri, Set.of(), HttpAuthorizationProvider.none());
        HttpConnector connector = connector(queryPin());
        ConnectorRuntimeContext runtime = runtime(connection);

        connector.start(runtime, configuration()).toCompletableFuture().join();
        connector.stop(runtime).toCompletableFuture().join();

        verify(borrowed, never()).close();
    }

    @Test
    void requiresHttpsBeforeAHostMayDeclareSecurityCapabilities() {
        HttpClient client = mock(HttpClient.class);
        when(client.followRedirects()).thenReturn(HttpClient.Redirect.NEVER);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
            () -> new HttpClientConnection(client, baseUri, Set.of("oauth2"), HttpAuthorizationProvider.none()));

        assertTrue(failure.getMessage().contains("must use HTTPS"));
    }

    @Test
    void anInFlightDispatchKeepsItsConfigurationSnapshotWhenTheBindingStops() throws Exception {
        AtomicReference<HttpRequest> seen = new AtomicReference<>();
        HttpClient client = successfulClient(seen, "{\"value\":\"recorded\"}");
        HttpConnector connector = connector(commandPinWithHostAuth("/snapshot"));
        AtomicReference<ConnectorRuntimeContext> runtime = new AtomicReference<>();
        HttpAuthorizationProvider authorization = request -> {
            connector.stop(runtime.get()).toCompletableFuture().join();
            return CompletableFuture.completedStage(HttpAuthorizationMaterial.none());
        };
        ConnectorRuntimeContext activeRuntime = runtime(connection(client, URI.create("https://api.example.test/api"),
            Set.of("host-auth"), authorization));
        runtime.set(activeRuntime);
        connector.start(activeRuntime, configuration()).toCompletableFuture().join();

        CommandOutcome<LookupOutput> outcome = command(connector).dispatch(new CommandInvocation<>(
            new RecordInput("new"), ConnectorConfigurationDocument.empty(), LookupOutput.class,
            org.pipelineframework.connector.ConnectorExecutionContext.empty(),
            Optional.of(new CommandDispatchIdentity("command", "occurrence", "attempt"))))
            .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertEquals(new LookupOutput("recorded"), assertInstanceOf(CommandOutcome.Succeeded.class, outcome).output());
        assertEquals("https://api.example.test/api/snapshot", seen.get().uri().toASCIIString());
    }

    private CommandOutcome<LookupOutput> dispatch(HttpOperationPin pin) throws Exception {
        HttpConnector connector = connector(pin);
        connector.start(runtime(connection(Set.of(), HttpAuthorizationProvider.none())), configuration())
            .toCompletableFuture().join();
        return command(connector).dispatch(new CommandInvocation<>(new RecordInput("new"),
            ConnectorConfigurationDocument.empty(), LookupOutput.class,
            org.pipelineframework.connector.ConnectorExecutionContext.empty(),
            Optional.of(new CommandDispatchIdentity("command", "occurrence", "attempt"))))
            .toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    private HttpConnector connector(HttpOperationPin pin) {
        return new HttpConnector(new HttpOperationCatalog(List.of(pin)), directBindings(pin), manifests(pin, pin.outputType()),
            getClass().getClassLoader());
    }

    private static HttpOperationBindingCatalog directBindings(HttpOperationPin pin) {
        String responseMapping = pin.responses().stream().map(HttpResponsePin::mappingKey)
            .flatMap(Optional::stream).findFirst().orElseThrow();
        var bindings = new java.util.ArrayList<>(List.of(
            new HttpOperationRepresentationBinding(pin.requestMappingKey(), HttpRepresentationMode.DIRECT,
                Optional.empty(), Optional.empty(), DIRECT),
            new HttpOperationRepresentationBinding(responseMapping, HttpRepresentationMode.DIRECT,
                Optional.empty(), Optional.empty(), "3".repeat(64))));
        pin.callbacks().forEach(callback -> bindings.add(new HttpOperationRepresentationBinding(callback.requestMappingKey(),
            HttpRepresentationMode.DIRECT, Optional.empty(), Optional.empty(), DIRECT)));
        return new HttpOperationBindingCatalog(bindings);
    }

    private static ConnectorProviderManifestCatalog manifests(HttpOperationPin pin, String outputType) {
        ConnectorOperationDescriptor operation = pin.kind().equals(ConnectorOperationKind.QUERY)
            ? new ConnectorOperationDescriptor(pin.operation(), pin.kind(), pin.majorVersion(), Optional.empty(),
                Optional.empty(), Optional.of(QueryCapabilities.conservative()), Optional.of(QueryOperationCardinality.ONE_TO_ONE),
                Optional.of(new ConnectorOperationTypeContract(pin.inputType(), Optional.of(outputType))))
            : new ConnectorOperationDescriptor(pin.operation(), pin.kind(), pin.majorVersion(), Optional.empty(),
                Optional.of(new CommandCapabilities(true, pin.providerIdempotencyKey().isPresent(), false,
                    CommandExecutionPosture.UNSPECIFIED, CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED,
                    false, Set.of())), Optional.empty(), Optional.empty(),
                Optional.of(new ConnectorOperationTypeContract(pin.inputType(), Optional.of(outputType))),
                pin.callbacks().stream().map(HttpCallbackPin::descriptor).toList());
        var provider = new ConnectorProviderArtifactDescriptor(new ConnectorProviderDescriptor(HttpConnector.PROVIDER_ID,
            new ConnectorProviderVersion(1, 0)), List.of(operation));
        return new ConnectorProviderManifestCatalog(List.of(new ConnectorProviderManifest(
            ConnectorProviderManifest.CURRENT_SCHEMA_VERSION, List.of(provider))));
    }

    private ConnectorRuntimeContext runtime(HttpClientConnection connection) {
        ConnectionResolver resolver = new ConnectionResolver() {
            @Override
            public <C extends ResolvedConnection> java.util.concurrent.CompletionStage<C> resolve(
                ConnectionResolutionRequest<C> request
            ) {
                return CompletableFuture.completedStage(request.connectionType().cast(connection));
            }
        };
        return ConnectorRuntimeContext.of("test", Runnable::run, Clock.systemUTC(), Optional.of(resolver));
    }

    private HttpClientConnection connection(Set<String> security, HttpAuthorizationProvider authorization) {
        return connection(HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build(), baseUri,
            security, authorization);
    }

    private static HttpClientConnection connection(
        HttpClient client,
        URI baseUri,
        Set<String> security,
        HttpAuthorizationProvider authorization
    ) {
        return new HttpClientConnection(client, baseUri, security, authorization);
    }

    private static HttpProviderConfiguration configuration() {
        return new HttpProviderConfiguration(new ConnectionRef("proof-http"), Optional.empty(), Optional.empty(),
            Optional.empty());
    }

    @SuppressWarnings("unchecked")
    private static QueryOperation<LookupInput, ConnectorConfigurationDocument, LookupOutput> query(HttpConnector connector) {
        return (QueryOperation<LookupInput, ConnectorConfigurationDocument, LookupOutput>) connector.operations()
            .stream().filter(operation -> operation.id().equals("evidence.lookup")).findFirst().orElseThrow();
    }

    @SuppressWarnings("unchecked")
    private static CommandOperation<RecordInput, ConnectorConfigurationDocument, LookupOutput> command(
        HttpConnector connector
    ) {
        return (CommandOperation<RecordInput, ConnectorConfigurationDocument, LookupOutput>) connector.operations()
            .stream().filter(operation -> operation.id().equals("evidence.record")).findFirst().orElseThrow();
    }

    private static HttpOperationPin queryPin() {
        return new HttpOperationPin("evidence.lookup", ConnectorOperationKind.QUERY, 1,
            LookupInput.class.getName(), LookupOutput.class.getName(), "POST", "/evidence/{subject}/lookup",
            List.of(
                new HttpParameterPin("subject", HttpParameterLocation.PATH, "subject", HttpParameterStyle.SIMPLE,
                    false, true, false, STRING),
                new HttpParameterPin("limit", HttpParameterLocation.QUERY, "limit", HttpParameterStyle.FORM,
                    true, true, false, INTEGER),
                new HttpParameterPin("X-Trace", HttpParameterLocation.HEADER, "trace", HttpParameterStyle.SIMPLE,
                    false, true, false, STRING),
                new HttpParameterPin("session", HttpParameterLocation.COOKIE, "session", HttpParameterStyle.FORM,
                    true, true, false, STRING)),
            Optional.of(new HttpRequestBodyPin("application/json", Optional.of("body"), true, OBJECT)),
            List.of(new HttpResponsePin("200", Optional.of("application/json"), HttpResponseOutcome.RESULT,
                Optional.of("http.lookup.response"), Optional.empty(), Optional.empty(), Optional.of(OBJECT))),
            new HttpSecurityConstraint(List.of(new HttpSecurityRequirement("oauth2", List.of("read"), List.of(
                new HttpAuthorizationTarget(HttpParameterLocation.HEADER, "Authorization"),
                new HttpAuthorizationTarget(HttpParameterLocation.QUERY, "api_key"))))),
            OBJECT, "http.lookup.request", Optional.empty(), SOURCE);
    }

    private static HttpOperationPin queryPinWithResponseSpecificity() {
        HttpOperationPin base = queryPin();
        return new HttpOperationPin(base.operation(), base.kind(), base.majorVersion(), base.inputType(),
            base.outputType(), base.method(), base.relativePathTemplate(), base.parameters(), base.requestBody(),
            List.of(new HttpResponsePin("200", Optional.empty(), HttpResponseOutcome.EMPTY,
                    Optional.empty(), Optional.of("media-agnostic-empty"), Optional.empty(), Optional.empty()),
                base.responses().getFirst()),
            HttpSecurityConstraint.none(), base.requestSchema(), base.requestMappingKey(),
            base.providerIdempotencyKey(), base.sourceFingerprint());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static HttpClient successfulClient(AtomicReference<HttpRequest> seen, String body) {
        HttpClient client = mock(HttpClient.class);
        when(client.followRedirects()).thenReturn(HttpClient.Redirect.NEVER);
        HttpResponse<byte[]> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(200);
        when(response.headers()).thenReturn(HttpHeaders.of(
            Map.of("Content-Type", List.of("application/json")), (left, right) -> true));
        when(response.body()).thenReturn(body.getBytes(StandardCharsets.UTF_8));
        when(response.uri()).thenAnswer(ignored -> seen.get().uri());
        doAnswer(invocation -> {
            seen.set(invocation.getArgument(0));
            return CompletableFuture.completedFuture(response);
        }).when(client).sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
        return client;
    }

    private static HttpOperationPin commandPin(String path) {
        return new HttpOperationPin("evidence.record", ConnectorOperationKind.COMMAND, 1,
            RecordInput.class.getName(), LookupOutput.class.getName(), "POST", path, List.of(),
            Optional.of(new HttpRequestBodyPin("application/json", Optional.empty(), true, OBJECT)),
            List.of(new HttpResponsePin("200", Optional.of("application/json"), HttpResponseOutcome.SUCCEEDED,
                Optional.of("http.record.response"), Optional.empty(),
                Optional.of(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED), Optional.of(OBJECT))),
            HttpSecurityConstraint.none(), OBJECT, "http.record.request",
            Optional.of(new HttpProviderIdempotencyKeyTarget(HttpParameterLocation.HEADER, "Idempotency-Key")), SOURCE);
    }

    private static HttpOperationPin commandPinWithStructuredFailure(String path) {
        HttpOperationPin base = commandPin(path);
        HttpWireSchema failure = new HttpWireSchema("""
            {
              "type": "object",
              "properties": { "code": { "type": "string" } },
              "required": [ "code" ],
              "additionalProperties": false
            }
            """);
        return new HttpOperationPin(base.operation(), base.kind(), base.majorVersion(), base.inputType(),
            base.outputType(), base.method(), base.relativePathTemplate(), base.parameters(), base.requestBody(),
            List.of(base.responses().getFirst(), new HttpResponsePin("409", Optional.of("application/json"),
                HttpResponseOutcome.TERMINAL_FAILURE, Optional.empty(), Optional.of("evidence-conflict"),
                Optional.empty(), Optional.of(failure))),
            base.security(), base.requestSchema(), base.requestMappingKey(), base.providerIdempotencyKey(),
            base.sourceFingerprint());
    }

    private static HttpOperationPin commandPinWithHostAuth(String path) {
        HttpOperationPin base = commandPin(path);
        HttpSecurityConstraint security = new HttpSecurityConstraint(List.of(new HttpSecurityRequirement(
            "host-auth", List.of(), List.of(new HttpAuthorizationTarget(
                HttpParameterLocation.HEADER, "Authorization")))));
        return new HttpOperationPin(base.operation(), base.kind(), base.majorVersion(), base.inputType(),
            base.outputType(), base.method(), base.relativePathTemplate(), base.parameters(), base.requestBody(),
            base.responses(), security, base.requestSchema(), base.requestMappingKey(),
            base.providerIdempotencyKey(), base.sourceFingerprint());
    }

    private static void respond(HttpExchange exchange, int status, String contentType, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    record LookupInput(String subject, int limit, String trace, String session, LookupBody body) {
    }

    record LookupBody(String phrase) {
    }

    record RecordInput(String value) {
    }

    record LookupOutput(String value) {
    }

    private static final class MapBuilder {
        private MapBuilder() {
        }

        static java.util.Map<String, List<String>> headers(String name, String value) {
            return values(name, value);
        }

        static java.util.Map<String, List<String>> values(String name, String value) {
            return java.util.Map.of(name, List.of(value));
        }
    }
}
