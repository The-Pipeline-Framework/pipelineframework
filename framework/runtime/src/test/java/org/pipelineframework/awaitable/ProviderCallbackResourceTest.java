package org.pipelineframework.awaitable;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import jakarta.enterprise.inject.Instance;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedHashMap;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.connector.*;
import org.pipelineframework.connector.http.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ProviderCallbackResourceTest {
    @org.junit.jupiter.api.io.TempDir java.nio.file.Path directory;
    public record Payload(String status) { }
    public static class Authenticator implements ProviderCallbackAuthenticator {
        public java.util.concurrent.CompletionStage<Optional<ProviderCallbackActor>> authenticate(
            ProviderCallbackAuthenticationRequest request) {
            return CompletableFuture.completedFuture(Optional.of(new ProviderCallbackActor("provider")));
        }
    }

    private final ProviderCallbackResource resource = new ProviderCallbackResource();
    private final HttpHeaders headers = mock(HttpHeaders.class);

    ProviderCallbackResourceTest() {
        resource.coordinator = mock(AwaitCoordinator.class);
        resource.executions = mock(PipelineExecutionService.class);
        resource.authenticators = mock(Instance.class);
        when(headers.getMediaType()).thenReturn(MediaType.APPLICATION_JSON_TYPE);
        when(headers.getRequestHeaders()).thenReturn(new MultivaluedHashMap<>());
    }

    @Test
    void boundsRawMaterialBeforeLookup() {
        assertEquals(400, request("x".repeat(8193), "{}").getStatus());
        assertEquals(400, request("token", "x".repeat(1_048_577)).getStatus());
        when(headers.getMediaType()).thenReturn(MediaType.TEXT_PLAIN_TYPE);
        assertEquals(400, request("token", "{}").getStatus());
        when(headers.getMediaType()).thenReturn(MediaType.APPLICATION_JSON_TYPE);
        var oversized = new MultivaluedHashMap<String, String>();
        oversized.add("Signature", "x".repeat(8193));
        when(headers.getRequestHeaders()).thenReturn(oversized);
        assertEquals(400, request("token", "{}").getStatus());
        verifyNoInteractions(resource.coordinator, resource.executions, resource.authenticators);
    }

    @Test
    void rejectsUnknownTokenWithoutDisclosingFailure() {
        when(resource.coordinator.resolveCallback(anyString(), anyLong()))
            .thenReturn(Uni.createFrom().failure(new AwaitResumeTokenRejectedException("secret diagnostic")));
        var response = request("token", "{}");
        assertEquals(400, response.getStatus());
        assertFalse(response.hasEntity());
        verifyNoInteractions(resource.executions, resource.authenticators);
    }

    @Test
    void allowsProviderRetryWhenDurableLookupFails() {
        when(resource.coordinator.resolveCallback(anyString(), anyLong()))
            .thenReturn(Uni.createFrom().failure(new IllegalStateException("internal diagnostic")));
        var response = request("token", "{}");
        assertEquals(503, response.getStatus());
        assertFalse(response.hasEntity());
        verifyNoInteractions(resource.executions, resource.authenticators);
    }

    @Test
    void authenticatesBeforeMappingAndAcknowledgesOnlyAdmission() throws Exception {
        var record = mock(AwaitInteractionRecord.class);
        when(record.status()).thenReturn(AwaitInteractionStatus.DISPATCHING);
        when(record.tenantId()).thenReturn("tenant");
        when(record.interactionId()).thenReturn("interaction");
        when(record.correlationId()).thenReturn("correlation");
        var schema = new HttpWireSchema("""
            {"type":"object","properties":{"status":{"type":"string"}},"required":["status"],"additionalProperties":false}
            """);
        var callback = new HttpCallbackPin("job.completed", "job.start", 1,
            new HttpCallbackInjectionTarget(HttpCallbackInjectionTarget.Location.BODY, List.of("callbackUrl"), Optional.empty()),
            "POST", "application/json", schema, "http.job.completed", Payload.class.getName(),
            new HttpSecurityConstraint(List.of(new HttpSecurityRequirement("callbackSignature", List.of(),
                List.of(new HttpAuthorizationTarget(HttpParameterLocation.HEADER, "X-Signature"))))),
            202, true, "a".repeat(64));
        var requestSchema = new HttpWireSchema("""
            {"type":"object","properties":{"callbackUrl":{"type":"string","format":"uri"}},"required":["callbackUrl"]}
            """);
        var operation = new HttpOperationPin("job.start", ConnectorOperationKind.COMMAND, 1, "Request", "Accepted",
            "POST", "/jobs", List.of(), Optional.of(new HttpRequestBodyPin("application/json", Optional.empty(), true, requestSchema)),
            List.of(new HttpResponsePin("202", Optional.of("application/json"), HttpResponseOutcome.SUCCEEDED,
                Optional.of("http.job.accepted"), Optional.empty(), Optional.of(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED),
                Optional.of(schema))), HttpSecurityConstraint.none(), requestSchema, "http.job.request", Optional.empty(),
            List.of(callback), "a".repeat(64));
        var bindings = new HttpOperationBindingCatalog(List.of(new HttpOperationRepresentationBinding(
            "http.job.completed", HttpRepresentationMode.DIRECT, Optional.empty(), Optional.empty(), "b".repeat(64))));
        var descriptor = mock(AwaitCompletionDescriptor.class);
        when(descriptor.transportOutputType()).thenReturn(Payload.class.getName());
        when(descriptor.callback()).thenReturn(Optional.of(new ConnectorCallbackSelection(ConnectorBindingName.of("jobs"),
            new ConnectorOperationIdentity(ConnectorProviderId.of("http.client"), "job.start", ConnectorOperationKind.COMMAND, 1),
            1, callback.descriptor(), "app.Endpoint", Authenticator.class.getName())));
        when(resource.coordinator.resolveCallback(anyString(), anyLong())).thenReturn(Uni.createFrom().item(record));
        when(resource.coordinator.callbackDescriptor(record)).thenReturn(descriptor);
        Instance<Authenticator> selected = mock(Instance.class);
        Authenticator authenticator = mock(Authenticator.class);
        when(resource.authenticators.select(Authenticator.class)).thenReturn(selected);
        when(selected.get()).thenReturn(authenticator);
        var provider = new ConnectorProviderArtifactDescriptor(new ConnectorProviderDescriptor(
            ConnectorProviderId.of("http.client"), new ConnectorProviderVersion(1, 0), Optional.empty()),
            List.of(new ConnectorOperationDescriptor("job.start", ConnectorOperationKind.COMMAND, 1,
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(new ConnectorOperationTypeContract("Request", Optional.of("Accepted"))),
                List.of(callback.descriptor()))), List.of());
        java.nio.file.Path metadata = directory.resolve("META-INF/pipeline");
        java.nio.file.Files.createDirectories(metadata);
        java.nio.file.Files.writeString(metadata.resolve("http-operations.json"), new HttpOperationCatalog(List.of(operation)).json());
        java.nio.file.Files.writeString(metadata.resolve("http-operation-bindings.json"), bindings.json());
        java.nio.file.Files.writeString(metadata.resolve("connector-providers.json"),
            ConnectorProviderArtifacts.json(new ConnectorProviderManifest(7, List.of(provider))));
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        var authenticationThread = new java.util.concurrent.atomic.AtomicReference<String>();
        var completionThread = new java.util.concurrent.atomic.AtomicReference<String>();
        var completionCommand = new java.util.concurrent.atomic.AtomicReference<AwaitCompletionCommand>();
        try (var loader = new java.net.URLClassLoader(new java.net.URL[]{directory.toUri().toURL()}, original);
             var ioExecutor = java.util.concurrent.Executors.newSingleThreadExecutor(task -> {
                 Thread thread = new Thread(task, "callback-test-io");
                 thread.setContextClassLoader(original);
                 return thread;
             })) {
            Thread.currentThread().setContextClassLoader(loader);
            assertEquals(List.of(operation), HttpOperationCatalog.load(loader).operations());
            assertEquals(bindings.bindings(), HttpOperationBindingCatalog.load(loader).bindings());
            when(resource.coordinator.resolveCallback(anyString(), anyLong()))
                .thenReturn(Uni.createFrom().item(record).emitOn(ioExecutor));
            when(authenticator.authenticate(any())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
            assertEquals(400, request("token", "not json").getStatus());
            verifyNoInteractions(resource.executions);
            when(authenticator.authenticate(any())).thenReturn(CompletableFuture.failedFuture(
                new IllegalStateException("authentication service unavailable")));
            assertEquals(503, request("token", "{}").getStatus());
            verifyNoInteractions(resource.executions);
            when(authenticator.authenticate(any())).thenAnswer(invocation -> {
                authenticationThread.set(Thread.currentThread().getName());
                return CompletableFuture.supplyAsync(() -> Optional.of(new ProviderCallbackActor("verified-provider")), ioExecutor);
            });
            assertEquals(400, request("token", "{}").getStatus());
            assertEquals(400, request("token", "{\"status\":\"done\"} {}").getStatus());
            verifyNoInteractions(resource.executions);
            when(resource.executions.completeAwaitInteraction(any())).thenAnswer(invocation -> {
                completionCommand.set(invocation.getArgument(0));
                completionThread.set(Thread.currentThread().getName());
                return Uni.createFrom().item(new AwaitCompletionResult(record, false));
            });
            var accepted = request("token", "{\"status\":\"done\"}");
            assertEquals(202, accepted.getStatus());
            assertNotNull(authenticationThread.get());
            assertNotNull(completionThread.get());
            assertNotEquals("callback-test-io", authenticationThread.get());
            assertNotEquals("callback-test-io", completionThread.get());
            assertEquals(new Payload("done"), completionCommand.get().responsePayload());
            assertEquals("verified-provider", completionCommand.get().actor());
            assertEquals("token", completionCommand.get().resumeToken());
            doReturn(Uni.createFrom().item(new AwaitCompletionResult(record, true)))
                .when(resource.executions).completeAwaitInteraction(any());
            assertEquals(202, request("token", "{\"status\":\"done\"}").getStatus());
            doReturn(Uni.createFrom().failure(new AwaitInteractionTerminalException("terminal")))
                .when(resource.executions).completeAwaitInteraction(any());
            assertEquals(400, request("token", "{\"status\":\"done\"}").getStatus());
            doReturn(Uni.createFrom().failure(new IllegalStateException("completion store unavailable")))
                .when(resource.executions).completeAwaitInteraction(any());
            assertEquals(503, request("token", "{\"status\":\"done\"}").getStatus());
            doThrow(new IllegalStateException("authenticator resolution unavailable")).when(selected).get();
            assertEquals(503, request("token", "{\"status\":\"done\"}").getStatus());
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    private jakarta.ws.rs.core.Response request(String token, String body) {
        return resource.complete(token, headers, new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)))
            .await().indefinitely();
    }
}
