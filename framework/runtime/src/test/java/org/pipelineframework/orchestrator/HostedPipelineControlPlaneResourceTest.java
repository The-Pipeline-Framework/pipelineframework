package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.dto.AwaitCompletionResponseDto;
import org.pipelineframework.awaitable.dto.AwaitInteractionDto;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;
import org.pipelineframework.orchestrator.dto.HostedAwaitCompletionRequest;
import org.pipelineframework.orchestrator.dto.HostedExecutionResultResponse;
import org.pipelineframework.orchestrator.dto.HostedExecutionSubmitRequest;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

class HostedPipelineControlPlaneResourceTest {

    private static final String TOKEN = "admin-token";
    private static final String AUTH = "Bearer " + TOKEN;

    private final JsonTransitionPayloadCodec payloadCodec = new JsonTransitionPayloadCodec();

    private HostedPipelineControlPlaneResource resource;
    private PipelineOrchestratorConfig config;
    private PipelineOrchestratorConfig.ControlPlaneConfig controlPlaneConfig;
    private PipelineControlPlane controlPlane;
    private PipelineExecutionService executionService;
    private PipelineBundleRegistry bundleRegistry;
    private PipelineBundleArtifactStore bundleArtifactStore;
    private PipelineWorkerAvailability workerAvailability;

    @BeforeEach
    void setUp() {
        resource = new HostedPipelineControlPlaneResource();
        config = mock(PipelineOrchestratorConfig.class);
        controlPlaneConfig = mock(PipelineOrchestratorConfig.ControlPlaneConfig.class);
        controlPlane = mock(PipelineControlPlane.class);
        executionService = mock(PipelineExecutionService.class);
        bundleRegistry = mock(PipelineBundleRegistry.class);
        bundleArtifactStore = mock(PipelineBundleArtifactStore.class);
        workerAvailability = mock(PipelineWorkerAvailability.class);
        resource.orchestratorConfig = config;
        resource.controlPlane = controlPlane;
        resource.executionService = executionService;
        resource.bundleRegistry = bundleRegistry;
        resource.bundleArtifactStore = bundleArtifactStore;
        resource.workerAvailability = workerAvailability;
        resource.payloadCodec = payloadCodec;
        resource.secretResolver = new LocalControlPlaneSecretResolver();
        when(config.controlPlane()).thenReturn(controlPlaneConfig);
        when(controlPlaneConfig.adminToken()).thenReturn(Optional.of(TOKEN));
        when(controlPlaneConfig.adminTokenRef()).thenReturn(Optional.empty());
    }

    @Test
    void rejectsCallsWhenDisabledByDefault() {
        when(controlPlaneConfig.enabled()).thenReturn(false);

        Response response = resource.getExecutionStatus("tenant-1", "exec-1", AUTH).await().indefinitely();

        assertEquals(404, response.getStatus());
    }

    @Test
    void startupValidationRequiresExactlyOneAdminTokenWhenEnabled() {
        when(controlPlaneConfig.enabled()).thenReturn(true);
        when(controlPlaneConfig.adminToken()).thenReturn(Optional.empty());
        when(controlPlaneConfig.adminTokenRef()).thenReturn(Optional.empty());

        assertThrows(IllegalStateException.class, resource::validateConfig);

        when(controlPlaneConfig.adminToken()).thenReturn(Optional.of(TOKEN));
        when(controlPlaneConfig.adminTokenRef()).thenReturn(Optional.of("sys:tpf.admin.token"));

        assertThrows(IllegalStateException.class, resource::validateConfig);
    }

    @Test
    void rejectsMissingOrInvalidBearerToken() {
        when(controlPlaneConfig.enabled()).thenReturn(true);

        Response missing = resource.getExecutionStatus("tenant-1", "exec-1", null).await().indefinitely();
        Response invalid = resource.getExecutionStatus("tenant-1", "exec-1", "Bearer wrong").await().indefinitely();

        assertEquals(401, missing.getStatus());
        assertEquals(401, invalid.getStatus());
    }

    @Test
    void acceptsAdminTokenReference() {
        when(controlPlaneConfig.enabled()).thenReturn(true);
        when(controlPlaneConfig.adminToken()).thenReturn(Optional.empty());
        when(controlPlaneConfig.adminTokenRef()).thenReturn(Optional.of("sys:tpf.hosted.admin"));
        System.setProperty("tpf.hosted.admin", TOKEN);
        when(controlPlane.getExecutionStatus("tenant-1", "exec-1"))
            .thenReturn(Uni.createFrom().item(status(ExecutionStatus.SUCCEEDED)));

        try {
            Response response = resource.getExecutionStatus("tenant-1", "exec-1", AUTH).await().indefinitely();

            assertEquals(200, response.getStatus());
        } finally {
            System.clearProperty("tpf.hosted.admin");
        }
    }

    @Test
    void submitExecutionDecodesPayloadAndDelegatesToControlPlane() {
        when(controlPlaneConfig.enabled()).thenReturn(true);
        RunAsyncAcceptedDto accepted = new RunAsyncAcceptedDto("exec-1", false, "/pipeline/executions/exec-1", 1L);
        PipelineBundleRecord bundle = bundleRecord();
        when(bundleRegistry.active("tenant-1", "org.example.restaurant"))
            .thenReturn(Uni.createFrom().item(Optional.of(bundle)));
        when(workerAvailability.check(any()))
            .thenReturn(Uni.createFrom().item(PipelineWorkerAvailabilityResult.available(
                "rest",
                new PipelineWorkerCapability(
                    PipelineWorkerCapability.PROTOCOL_VERSION,
                    "rest",
                    "org.example.restaurant",
                    "sha256:bundle",
                    "bundle",
                    List.of("application/tpf-transition-envelope+json"),
                    List.of("rest")))));
        when(controlPlane.executePipelineAsync(
                any(),
                eq("tenant-1"),
                eq("idem-1"),
                eq(false),
                eq("org.example.restaurant"),
                eq("sha256:bundle")))
            .thenReturn(Uni.createFrom().item(accepted));
        HostedExecutionSubmitRequest request = new HostedExecutionSubmitRequest(
            "org.example.restaurant",
            ExecutionInputShape.UNI,
            payloadCodec.encode("order"),
            "idem-1",
            false);

        Response response = resource.submitExecution("tenant-1", AUTH, request).await().indefinitely();

        assertEquals(200, response.getStatus());
        assertEquals(accepted, response.getEntity());
        verify(bundleArtifactStore).verify(bundle);
        verify(workerAvailability).check(argThat(availabilityRequest ->
            "tenant-1".equals(availabilityRequest.tenantId())
                && "org.example.restaurant".equals(availabilityRequest.pipelineId())
                && "sha256:bundle".equals(availabilityRequest.bundleVersionId())));
        verify(controlPlane).executePipelineAsync(
            argThat(input -> input instanceof Uni<?>),
            eq("tenant-1"),
            eq("idem-1"),
            eq(false),
            eq("org.example.restaurant"),
            eq("sha256:bundle"));
    }

    @Test
    void submitExecutionCoercesInputUsingActiveBundleManifest() {
        when(controlPlaneConfig.enabled()).thenReturn(true);
        RunAsyncAcceptedDto accepted = new RunAsyncAcceptedDto("exec-1", false, "/pipeline/executions/exec-1", 1L);
        PipelineBundleRecord bundle = bundleRecord(Integer.class.getName());
        when(bundleRegistry.active("tenant-1", "org.example.restaurant"))
            .thenReturn(Uni.createFrom().item(Optional.of(bundle)));
        when(workerAvailability.check(any()))
            .thenReturn(Uni.createFrom().item(PipelineWorkerAvailabilityResult.available(
                "rest",
                new PipelineWorkerCapability(
                    PipelineWorkerCapability.PROTOCOL_VERSION,
                    "rest",
                    "org.example.restaurant",
                    "sha256:bundle",
                    "bundle",
                    List.of("application/tpf-transition-envelope+json"),
                    List.of("rest")))));
        when(controlPlane.executePipelineAsync(
                any(),
                eq("tenant-1"),
                eq("idem-1"),
                eq(false),
                eq("org.example.restaurant"),
                eq("sha256:bundle")))
            .thenReturn(Uni.createFrom().item(accepted));
        HostedExecutionSubmitRequest request = new HostedExecutionSubmitRequest(
            "org.example.restaurant",
            ExecutionInputShape.UNI,
            payloadCodec.encode("42"),
            "idem-1",
            false);

        Response response = resource.submitExecution("tenant-1", AUTH, request).await().indefinitely();

        assertEquals(200, response.getStatus());
        AtomicReference<Object> capturedInput = new AtomicReference<>();
        verify(controlPlane).executePipelineAsync(
            argThat(input -> {
                capturedInput.set(input);
                return input instanceof Uni<?>;
            }),
            eq("tenant-1"),
            eq("idem-1"),
            eq(false),
            eq("org.example.restaurant"),
            eq("sha256:bundle"));
        Object item = assertInstanceOf(Uni.class, capturedInput.get()).await().indefinitely();
        assertEquals(42, item);
    }

    @Test
    void submitRequestRequiresPipelineIdShapeAndPayload() {
        SerializedTransitionPayload payload = payloadCodec.encode("order");

        assertThrows(IllegalArgumentException.class, () -> new HostedExecutionSubmitRequest(
            " ",
            ExecutionInputShape.UNI,
            payload,
            "idem-1",
            false));
        assertThrows(IllegalArgumentException.class, () -> new HostedExecutionSubmitRequest(
            "org.example.restaurant",
            null,
            payload,
            "idem-1",
            false));
        assertThrows(IllegalArgumentException.class, () -> new HostedExecutionSubmitRequest(
            "org.example.restaurant",
            ExecutionInputShape.UNI,
            null,
            "idem-1",
            false));
    }

    @Test
    void submitExecutionFailsWhenPipelineHasNoActiveBundle() {
        when(controlPlaneConfig.enabled()).thenReturn(true);
        when(bundleRegistry.active("tenant-1", "org.example.restaurant"))
            .thenReturn(Uni.createFrom().item(Optional.empty()));
        HostedExecutionSubmitRequest request = new HostedExecutionSubmitRequest(
            "org.example.restaurant",
            ExecutionInputShape.UNI,
            payloadCodec.encode("order"),
            "idem-1",
            false);

        Response response = resource.submitExecution("tenant-1", AUTH, request).await().indefinitely();

        assertEquals(409, response.getStatus());
        verify(controlPlane, never()).executePipelineAsync(
            any(), anyString(), anyString(), eq(false), anyString(), anyString());
    }

    @Test
    void submitExecutionFailsWhenActiveBundleArtifactIsUnavailable() {
        when(controlPlaneConfig.enabled()).thenReturn(true);
        PipelineBundleRecord bundle = bundleRecord();
        when(bundleRegistry.active("tenant-1", "org.example.restaurant"))
            .thenReturn(Uni.createFrom().item(Optional.of(bundle)));
        doThrow(new IllegalStateException("Stored bundle artifact is missing or unreadable"))
            .when(bundleArtifactStore).verify(bundle);
        HostedExecutionSubmitRequest request = new HostedExecutionSubmitRequest(
            "org.example.restaurant",
            ExecutionInputShape.UNI,
            payloadCodec.encode("order"),
            "idem-1",
            false);

        Response response = resource.submitExecution("tenant-1", AUTH, request).await().indefinitely();

        assertEquals(409, response.getStatus());
        verify(controlPlane, never()).executePipelineAsync(
            any(), anyString(), anyString(), eq(false), anyString(), anyString());
    }

    @Test
    void submitExecutionFailsWhenNoWorkerHostsActiveBundle() {
        when(controlPlaneConfig.enabled()).thenReturn(true);
        PipelineBundleRecord bundle = bundleRecord();
        when(bundleRegistry.active("tenant-1", "org.example.restaurant"))
            .thenReturn(Uni.createFrom().item(Optional.of(bundle)));
        when(workerAvailability.check(any()))
            .thenReturn(Uni.createFrom().item(PipelineWorkerAvailabilityResult.unavailable(
                "rest",
                "Worker hosts a different bundle")));
        HostedExecutionSubmitRequest request = new HostedExecutionSubmitRequest(
            "org.example.restaurant",
            ExecutionInputShape.UNI,
            payloadCodec.encode("order"),
            "idem-1",
            false);

        Response response = resource.submitExecution("tenant-1", AUTH, request).await().indefinitely();

        assertEquals(503, response.getStatus());
        verify(controlPlane, never()).executePipelineAsync(
            any(), anyString(), anyString(), eq(false), anyString(), anyString());
    }

    @Test
    void getResultReturnsStatusAndSerializedPayloadWhenSucceeded() {
        when(controlPlaneConfig.enabled()).thenReturn(true);
        when(controlPlane.getExecutionStatus("tenant-1", "exec-1"))
            .thenReturn(Uni.createFrom().item(status(ExecutionStatus.SUCCEEDED)));
        when(controlPlane.getExecutionResultPayload("tenant-1", "exec-1"))
            .thenReturn(Uni.createFrom().item("approved"));

        Response response = resource.getExecutionResult("tenant-1", "exec-1", AUTH).await().indefinitely();

        assertEquals(200, response.getStatus());
        HostedExecutionResultResponse result = assertInstanceOf(HostedExecutionResultResponse.class, response.getEntity());
        assertEquals(ExecutionStatus.SUCCEEDED, result.status().status());
        assertEquals("approved", payloadCodec.decode(result.resultPayload()));
    }

    @Test
    void getResultReturnsStoredSerializedPayloadWithoutDoubleEncoding() {
        when(controlPlaneConfig.enabled()).thenReturn(true);
        when(controlPlane.getExecutionStatus("tenant-1", "exec-1"))
            .thenReturn(Uni.createFrom().item(status(ExecutionStatus.SUCCEEDED)));
        SerializedTransitionPayload serialized = payloadCodec.encode("approved");
        when(controlPlane.getExecutionResultPayload("tenant-1", "exec-1"))
            .thenReturn(Uni.createFrom().item(serialized));

        Response response = resource.getExecutionResult("tenant-1", "exec-1", AUTH).await().indefinitely();

        assertEquals(200, response.getStatus());
        HostedExecutionResultResponse result = assertInstanceOf(HostedExecutionResultResponse.class, response.getEntity());
        assertEquals(serialized, result.resultPayload());
    }

    @Test
    void getResultReturnsOnlyStatusWhenExecutionIsNotSucceeded() {
        when(controlPlaneConfig.enabled()).thenReturn(true);
        when(controlPlane.getExecutionStatus("tenant-1", "exec-1"))
            .thenReturn(Uni.createFrom().item(status(ExecutionStatus.WAITING_EXTERNAL)));

        Response response = resource.getExecutionResult("tenant-1", "exec-1", AUTH).await().indefinitely();

        assertEquals(200, response.getStatus());
        HostedExecutionResultResponse result = assertInstanceOf(HostedExecutionResultResponse.class, response.getEntity());
        assertEquals(ExecutionStatus.WAITING_EXTERNAL, result.status().status());
        assertNull(result.resultPayload());
        verify(controlPlane, never()).getExecutionResultPayload(anyString(), anyString());
    }

    @Test
    void pendingInteractionsDelegatesAndMapsDtos() {
        when(controlPlaneConfig.enabled()).thenReturn(true);
        AwaitInteractionRecord record = awaitInteraction(
            AwaitInteractionStatus.WAITING,
            Map.of("orderId", "order-1"),
            null,
            null);
        when(controlPlane.queryPendingAwaitInteractions("tenant-1", null, null, "AwaitDecision", 25))
            .thenReturn(Uni.createFrom().item(List.of(record)));

        Response response = resource.pendingInteractions("tenant-1", AUTH, null, null, "AwaitDecision", 25).await().indefinitely();

        assertEquals(200, response.getStatus());
        List<?> interactions = assertInstanceOf(List.class, response.getEntity());
        AwaitInteractionDto dto = assertInstanceOf(AwaitInteractionDto.class, interactions.getFirst());
        assertEquals("interaction-1", dto.interactionId());
    }

    @Test
    void completeInteractionDecodesPayloadAndDelegates() {
        when(controlPlaneConfig.enabled()).thenReturn(true);
        AwaitInteractionRecord record = awaitInteraction(
            AwaitInteractionStatus.COMPLETED,
            Map.of(),
            Map.of("decision", "accepted"),
            "actor-1");
        when(executionService.completeAwaitInteraction(any()))
            .thenReturn(Uni.createFrom().item(new AwaitCompletionResult(record, false)));
        HostedAwaitCompletionRequest request = new HostedAwaitCompletionRequest(
            "interaction-1",
            null,
            null,
            "complete-1",
            payloadCodec.encode(Map.of("decision", "accepted")),
            "actor-1");

        Response response = resource.completeInteraction("tenant-1", AUTH, request).await().indefinitely();

        assertEquals(200, response.getStatus());
        AwaitCompletionResponseDto dto = assertInstanceOf(AwaitCompletionResponseDto.class, response.getEntity());
        assertEquals("interaction-1", dto.interactionId());
        verify(executionService).completeAwaitInteraction(argThat(command ->
            command instanceof AwaitCompletionCommand
                && "tenant-1".equals(command.tenantId())
                && "interaction-1".equals(command.interactionId())
                && "complete-1".equals(command.idempotencyKey())
                && command.responsePayload() instanceof Map<?, ?>));
    }

    @Test
    void completionRequestRequiresLookupHandleAndPayload() {
        SerializedTransitionPayload payload = payloadCodec.encode(Map.of("decision", "accepted"));

        assertThrows(IllegalArgumentException.class, () -> new HostedAwaitCompletionRequest(
            null,
            null,
            null,
            "complete-1",
            payload,
            "actor-1"));
        assertThrows(IllegalArgumentException.class, () -> new HostedAwaitCompletionRequest(
            "interaction-1",
            null,
            null,
            "complete-1",
            null,
            "actor-1"));
    }

    private static ExecutionStatusDto status(ExecutionStatus status) {
        return new ExecutionStatusDto("exec-1", status, 0, 0, 1L, 0L, 1L, null, null);
    }

    private static PipelineBundleRecord bundleRecord() {
        return bundleRecord(String.class.getName());
    }

    private static PipelineBundleRecord bundleRecord(String inputTypeId) {
        PipelineBundleManifest manifest = new PipelineBundleManifest(
            PipelineBundleManifest.CURRENT_SCHEMA_VERSION,
            "org.example.restaurant",
            "sha256:bundle",
            "bundle",
            "COMPUTE",
            "REST",
            "monolith-svc",
            false,
            "monolith",
            List.of(new PipelineBundleStepDescriptor(
                0,
                "Validate",
                "service",
                "ONE_TO_ONE",
                inputTypeId,
                "Output",
                "Runtime",
                "Client",
                null)),
            PipelineBundleCapabilities.defaults());
        return new PipelineBundleRecord(
            "tenant-1",
            "org.example.restaurant",
            "sha256:bundle",
            "bundle",
            "/tmp/bundle.jar",
            123L,
            "sha256:artifact",
            PipelineBundleStatus.ACTIVE,
            manifest,
            1L,
            1L,
            1L);
    }

    private static AwaitInteractionRecord awaitInteraction(
        AwaitInteractionStatus status,
        Map<String, Object> requestPayload,
        Map<String, Object> responsePayload,
        String actor
    ) {
        return new AwaitInteractionRecord(
            "tenant-1",
            "exec-1",
            "AwaitDecision",
            2,
            "Decision",
            "interaction-1",
            "corr-1",
            "cause-1",
            "idem-1",
            1L,
            status,
            requestPayload,
            responsePayload,
            "unit-1",
            null,
            actor,
            null,
            null,
            "interaction-api",
            Map.of(),
            10_000L,
            1_000L,
            status == AwaitInteractionStatus.COMPLETED ? 2_000L : 1_000L,
            9_999L);
    }
}
