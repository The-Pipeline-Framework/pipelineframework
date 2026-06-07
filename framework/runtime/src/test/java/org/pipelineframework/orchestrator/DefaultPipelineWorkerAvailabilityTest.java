package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultPipelineWorkerAvailabilityTest {

    private DefaultPipelineWorkerAvailability availability;
    private PipelineOrchestratorConfig config;
    private PipelineOrchestratorConfig.RestWorkerConfig restConfig;
    private PipelineOrchestratorConfig.GrpcWorkerConfig grpcConfig;
    private PipelineOrchestratorConfig.SqsWorkerConfig sqsConfig;
    private PipelineBundleIdentityResolver identityResolver;
    private RestPipelineTransitionWorker restWorker;
    private GrpcPipelineTransitionWorker grpcWorker;

    @BeforeEach
    void setUp() {
        availability = new DefaultPipelineWorkerAvailability();
        config = mock(PipelineOrchestratorConfig.class);
        restConfig = mock(PipelineOrchestratorConfig.RestWorkerConfig.class);
        grpcConfig = mock(PipelineOrchestratorConfig.GrpcWorkerConfig.class);
        sqsConfig = mock(PipelineOrchestratorConfig.SqsWorkerConfig.class);
        identityResolver = mock(PipelineBundleIdentityResolver.class);
        restWorker = mock(RestPipelineTransitionWorker.class);
        grpcWorker = mock(GrpcPipelineTransitionWorker.class);
        availability.orchestratorConfig = config;
        availability.identityResolver = identityResolver;
        availability.restWorker = restWorker;
        availability.grpcWorker = grpcWorker;
        when(config.workerRest()).thenReturn(restConfig);
        when(config.workerGrpc()).thenReturn(grpcConfig);
        when(config.workerSqs()).thenReturn(sqsConfig);
        when(restConfig.isEnabled()).thenReturn(false);
        when(grpcConfig.isEnabled()).thenReturn(false);
        when(sqsConfig.isEnabled()).thenReturn(false);
        when(sqsConfig.pipelineId()).thenReturn(Optional.empty());
        when(sqsConfig.contractVersion()).thenReturn(Optional.empty());
        when(sqsConfig.releaseVersion()).thenReturn(Optional.empty());
        when(sqsConfig.bundleVersionId()).thenReturn(Optional.empty());
        when(identityResolver.pipelineId(config)).thenReturn("org.example.restaurant");
        when(identityResolver.contractVersion()).thenReturn("sha256:bundle");
        when(identityResolver.releaseVersion(config)).thenReturn("sha256:bundle");
        when(identityResolver.bundleVersionId(config)).thenReturn("sha256:bundle");
        when(identityResolver.bundleHash()).thenReturn("bundle");
        when(identityResolver.capabilities()).thenReturn(PipelineBundleCapabilities.defaults());
    }

    @Test
    void localWorkerAvailabilityMatchesGeneratedIdentity() {
        PipelineWorkerAvailabilityResult result = availability.check(new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example.restaurant",
            "sha256:bundle",
            "sha256:bundle",
            "sha256:bundle")).await().atMost(Duration.ofSeconds(2));

        assertTrue(result.available());
    }

    @Test
    void localWorkerAvailabilityRejectsMismatchedBundle() {
        PipelineWorkerAvailabilityResult result = availability.check(new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example.restaurant",
            "sha256:other")).await().atMost(Duration.ofSeconds(2));

        assertFalse(result.available());
    }

    @Test
    void sqsWorkerAvailabilityRequiresStaticIdentity() {
        when(sqsConfig.isEnabled()).thenReturn(true);
        when(sqsConfig.pipelineId()).thenReturn(Optional.empty());
        when(sqsConfig.contractVersion()).thenReturn(Optional.empty());
        when(sqsConfig.releaseVersion()).thenReturn(Optional.empty());
        when(sqsConfig.bundleVersionId()).thenReturn(Optional.empty());

        PipelineWorkerAvailabilityResult result = availability.check(new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example.restaurant",
            "sha256:bundle",
            "sha256:bundle",
            "sha256:bundle")).await().atMost(Duration.ofSeconds(2));

        assertFalse(result.available());
    }

    @Test
    void sqsWorkerAvailabilityMatchesConfiguredStaticIdentity() {
        when(sqsConfig.isEnabled()).thenReturn(true);
        when(sqsConfig.pipelineId()).thenReturn(Optional.of("org.example.restaurant"));
        when(sqsConfig.contractVersion()).thenReturn(Optional.of("sha256:bundle"));
        when(sqsConfig.releaseVersion()).thenReturn(Optional.of("sha256:bundle"));
        when(sqsConfig.bundleVersionId()).thenReturn(Optional.of("sha256:bundle"));

        PipelineWorkerAvailabilityResult result = availability.check(new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example.restaurant",
            "sha256:bundle",
            "sha256:bundle",
            "sha256:bundle")).await().atMost(Duration.ofSeconds(2));

        assertTrue(result.available());
    }

    @Test
    void restWorkerAvailabilityMatchesRemoteCapability() {
        when(restConfig.isEnabled()).thenReturn(true);
        when(restWorker.capabilities()).thenReturn(Uni.createFrom().item(capability("rest", "sha256:bundle")));

        PipelineWorkerAvailabilityResult result = availability.check(new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example.restaurant",
            "sha256:bundle")).await().atMost(Duration.ofSeconds(2));

        assertTrue(result.available());
    }

    @Test
    void restWorkerAvailabilityRejectsMismatchedRemoteCapability() {
        when(restConfig.isEnabled()).thenReturn(true);
        when(restWorker.capabilities()).thenReturn(Uni.createFrom().item(capability("rest", "sha256:other")));

        PipelineWorkerAvailabilityResult result = availability.check(new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example.restaurant",
            "sha256:bundle")).await().atMost(Duration.ofSeconds(2));

        assertFalse(result.available());
    }

    @Test
    void grpcWorkerAvailabilityMatchesRemoteCapability() {
        when(grpcConfig.isEnabled()).thenReturn(true);
        when(grpcWorker.capabilities()).thenReturn(Uni.createFrom().item(capability("grpc", "sha256:bundle")));

        PipelineWorkerAvailabilityResult result = availability.check(new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example.restaurant",
            "sha256:bundle")).await().atMost(Duration.ofSeconds(2));

        assertTrue(result.available());
    }

    @Test
    void grpcWorkerAvailabilityRejectsMismatchedRemoteCapability() {
        when(grpcConfig.isEnabled()).thenReturn(true);
        when(grpcWorker.capabilities()).thenReturn(Uni.createFrom().item(capability("grpc", "sha256:other")));

        PipelineWorkerAvailabilityResult result = availability.check(new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example.restaurant",
            "sha256:bundle")).await().atMost(Duration.ofSeconds(2));

        assertFalse(result.available());
    }

    private static PipelineWorkerCapability capability(String providerName, String bundleVersionId) {
        return new PipelineWorkerCapability(
            PipelineWorkerCapability.PROTOCOL_VERSION,
            providerName,
            "org.example.restaurant",
            bundleVersionId,
            "bundle",
            List.of(TransitionPayloadEncoding.JSON),
            List.of(providerName));
    }
}
