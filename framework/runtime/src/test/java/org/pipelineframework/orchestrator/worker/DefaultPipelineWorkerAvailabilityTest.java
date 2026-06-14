package org.pipelineframework.orchestrator.worker;

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
import org.pipelineframework.orchestrator.GrpcPipelineTransitionWorker;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.RestPipelineTransitionWorker;
import org.pipelineframework.orchestrator.TransitionPayloadEncoding;

class DefaultPipelineWorkerAvailabilityTest {

    private DefaultPipelineWorkerAvailability availability;
    private PipelineOrchestratorConfig config;
    private PipelineOrchestratorConfig.RestWorkerConfig restConfig;
    private PipelineOrchestratorConfig.GrpcWorkerConfig grpcConfig;
    private PipelineOrchestratorConfig.SqsWorkerConfig sqsConfig;
    private PipelineReleaseIdentityResolver identityResolver;
    private RestPipelineTransitionWorker restWorker;
    private GrpcPipelineTransitionWorker grpcWorker;
    private InMemoryPipelineWorkerRegistry workerRegistry;

    @BeforeEach
    void setUp() {
        availability = new DefaultPipelineWorkerAvailability();
        config = mock(PipelineOrchestratorConfig.class);
        restConfig = mock(PipelineOrchestratorConfig.RestWorkerConfig.class);
        grpcConfig = mock(PipelineOrchestratorConfig.GrpcWorkerConfig.class);
        sqsConfig = mock(PipelineOrchestratorConfig.SqsWorkerConfig.class);
        identityResolver = mock(PipelineReleaseIdentityResolver.class);
        restWorker = mock(RestPipelineTransitionWorker.class);
        grpcWorker = mock(GrpcPipelineTransitionWorker.class);
        workerRegistry = new InMemoryPipelineWorkerRegistry();
        availability.orchestratorConfig = config;
        availability.identityResolver = identityResolver;
        availability.restWorker = restWorker;
        availability.grpcWorker = grpcWorker;
        availability.workerRegistry = workerRegistry;
        when(config.workerRest()).thenReturn(restConfig);
        when(config.workerGrpc()).thenReturn(grpcConfig);
        when(config.workerSqs()).thenReturn(sqsConfig);
        when(restConfig.isEnabled()).thenReturn(false);
        when(grpcConfig.isEnabled()).thenReturn(false);
        when(sqsConfig.isEnabled()).thenReturn(false);
        when(sqsConfig.pipelineId()).thenReturn(Optional.empty());
        when(sqsConfig.contractVersion()).thenReturn(Optional.empty());
        when(sqsConfig.releaseVersion()).thenReturn(Optional.empty());
        when(sqsConfig.artifactId()).thenReturn(Optional.empty());
        when(sqsConfig.artifactDigest()).thenReturn(Optional.empty());
        when(identityResolver.pipelineId(config)).thenReturn("org.example.restaurant");
        when(identityResolver.contractVersion()).thenReturn("sha256:contract");
        when(identityResolver.releaseVersion(config)).thenReturn("sha256:release");
        when(identityResolver.artifactId(config)).thenReturn("restaurant-approval-monolith");
        when(identityResolver.artifactDigest(config)).thenReturn("sha256:artifact");
        when(identityResolver.capabilities()).thenReturn(PipelineBundleCapabilities.defaults());
    }

    @Test
    void localWorkerAvailabilityMatchesGeneratedIdentity() {
        registerWorker("local", "sha256:release", "restaurant-approval-monolith", "sha256:artifact");

        PipelineWorkerAvailabilityResult result = availability.check(new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example.restaurant",
            "sha256:contract",
            "sha256:release",
            "restaurant-approval-monolith",
            "sha256:artifact")).await().atMost(Duration.ofSeconds(2));

        assertTrue(result.available());
    }

    @Test
    void localWorkerAvailabilityRequiresHealthyLifecycleRecord() {
        PipelineWorkerAvailabilityResult result = availability.check(new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example.restaurant",
            "sha256:contract",
            "sha256:release",
            "restaurant-approval-monolith",
            "sha256:artifact")).await().atMost(Duration.ofSeconds(2));

        assertFalse(result.available());
        assertTrue(result.message().contains("No worker lifecycle record"));
    }

    @Test
    void localWorkerAvailabilityRejectsMismatchedRelease() {
        registerWorker("local", "sha256:release", "restaurant-approval-monolith", "sha256:artifact");

        PipelineWorkerAvailabilityResult result = availability.check(new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example.restaurant",
            "sha256:contract",
            "sha256:other",
            "restaurant-approval-monolith",
            "sha256:artifact")).await().atMost(Duration.ofSeconds(2));

        assertFalse(result.available());
    }

    @Test
    void sqsWorkerAvailabilityRequiresStaticIdentity() {
        when(sqsConfig.isEnabled()).thenReturn(true);
        when(sqsConfig.pipelineId()).thenReturn(Optional.empty());
        when(sqsConfig.contractVersion()).thenReturn(Optional.empty());
        when(sqsConfig.releaseVersion()).thenReturn(Optional.empty());
        when(sqsConfig.artifactId()).thenReturn(Optional.empty());
        when(sqsConfig.artifactDigest()).thenReturn(Optional.empty());

        PipelineWorkerAvailabilityResult result = availability.check(new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example.restaurant",
            "sha256:contract",
            "sha256:release",
            "restaurant-approval-monolith",
            "sha256:artifact")).await().atMost(Duration.ofSeconds(2));

        assertFalse(result.available());
    }

    @Test
    void sqsWorkerAvailabilityMatchesConfiguredStaticIdentity() {
        when(sqsConfig.isEnabled()).thenReturn(true);
        when(sqsConfig.pipelineId()).thenReturn(Optional.of("org.example.restaurant"));
        when(sqsConfig.contractVersion()).thenReturn(Optional.of("sha256:contract"));
        when(sqsConfig.releaseVersion()).thenReturn(Optional.of("sha256:release"));
        when(sqsConfig.artifactId()).thenReturn(Optional.of("restaurant-approval-monolith"));
        when(sqsConfig.artifactDigest()).thenReturn(Optional.of("sha256:artifact"));
        registerWorker("sqs", "sha256:release", "restaurant-approval-monolith", "sha256:artifact");

        PipelineWorkerAvailabilityResult result = availability.check(new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example.restaurant",
            "sha256:contract",
            "sha256:release",
            "restaurant-approval-monolith",
            "sha256:artifact")).await().atMost(Duration.ofSeconds(2));

        assertTrue(result.available());
    }

    @Test
    void restWorkerAvailabilityMatchesRemoteCapability() {
        when(restConfig.isEnabled()).thenReturn(true);
        when(restWorker.capabilities()).thenReturn(Uni.createFrom().item(capability("rest", "sha256:release")));
        registerWorker("rest", "sha256:release", "", "");

        PipelineWorkerAvailabilityResult result = availability.check(new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example.restaurant",
            "sha256:contract",
            "sha256:release",
            "",
            "")).await().atMost(Duration.ofSeconds(2));

        assertTrue(result.available());
    }

    @Test
    void restWorkerAvailabilityRejectsMismatchedRemoteCapability() {
        when(restConfig.isEnabled()).thenReturn(true);
        when(restWorker.capabilities()).thenReturn(Uni.createFrom().item(capability("rest", "sha256:other")));
        registerWorker("rest", "sha256:release", "", "");

        PipelineWorkerAvailabilityResult result = availability.check(new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example.restaurant",
            "sha256:contract",
            "sha256:release",
            "",
            "")).await().atMost(Duration.ofSeconds(2));

        assertFalse(result.available());
    }

    @Test
    void grpcWorkerAvailabilityMatchesRemoteCapability() {
        when(grpcConfig.isEnabled()).thenReturn(true);
        when(grpcWorker.capabilities()).thenReturn(Uni.createFrom().item(capability("grpc", "sha256:release")));
        registerWorker("grpc", "sha256:release", "", "");

        PipelineWorkerAvailabilityResult result = availability.check(new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example.restaurant",
            "sha256:contract",
            "sha256:release",
            "",
            "")).await().atMost(Duration.ofSeconds(2));

        assertTrue(result.available());
    }

    @Test
    void grpcWorkerAvailabilityRejectsMismatchedRemoteCapability() {
        when(grpcConfig.isEnabled()).thenReturn(true);
        when(grpcWorker.capabilities()).thenReturn(Uni.createFrom().item(capability("grpc", "sha256:other")));
        registerWorker("grpc", "sha256:release", "", "");

        PipelineWorkerAvailabilityResult result = availability.check(new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example.restaurant",
            "sha256:contract",
            "sha256:release",
            "",
            "")).await().atMost(Duration.ofSeconds(2));

        assertFalse(result.available());
    }

    @Test
    void availabilityRejectsDrainingWorker() {
        registerWorker("local", "sha256:release", "restaurant-approval-monolith", "sha256:artifact");
        workerRegistry.markDraining(
            "tenant-1",
            "org.example.restaurant",
            "local-worker",
            System.currentTimeMillis(),
            Duration.ofMinutes(2)).await().atMost(Duration.ofSeconds(2));

        PipelineWorkerAvailabilityResult result = availability.check(new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example.restaurant",
            "sha256:contract",
            "sha256:release",
            "restaurant-approval-monolith",
            "sha256:artifact")).await().atMost(Duration.ofSeconds(2));

        assertFalse(result.available());
        assertTrue(result.message().contains("DRAINING"));
    }

    @Test
    void availabilityRejectsArtifactMismatchLifecycleRecord() {
        registerWorker("local", "sha256:release", "restaurant-approval-monolith", "sha256:other");

        PipelineWorkerAvailabilityResult result = availability.check(new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example.restaurant",
            "sha256:contract",
            "sha256:release",
            "restaurant-approval-monolith",
            "sha256:artifact")).await().atMost(Duration.ofSeconds(2));

        assertFalse(result.available());
        assertTrue(result.message().contains("artifact identity"));
    }

    private void registerWorker(
        String protocol,
        String releaseVersion,
        String artifactId,
        String artifactDigest) {
        workerRegistry.register(new PipelineWorkerRegistration(
            "tenant-1",
            "org.example.restaurant",
            "sha256:contract",
            releaseVersion,
            protocol + "-worker",
            protocol,
            protocol.equals("local") ? "in-process" : "http://localhost",
            artifactId,
            artifactDigest), System.currentTimeMillis()).await().atMost(Duration.ofSeconds(2));
    }

    private static PipelineWorkerCapability capability(String providerName, String releaseVersion) {
        return new PipelineWorkerCapability(
            PipelineWorkerCapability.PROTOCOL_VERSION,
            providerName,
            "org.example.restaurant",
            "sha256:contract",
            releaseVersion,
            "",
            "",
            List.of(TransitionPayloadEncoding.JSON),
            List.of(providerName));
    }
}
