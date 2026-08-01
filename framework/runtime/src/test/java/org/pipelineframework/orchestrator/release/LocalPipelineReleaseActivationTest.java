package org.pipelineframework.orchestrator.release;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;

import io.smallrye.mutiny.Uni;

class LocalPipelineReleaseActivationTest {

    @Test
    void activatesTheGeneratedContractForTheLocalInMemoryRelease() {
        PipelineContractDescriptor contract = new PipelineContractDescriptor(
            1, "consumer-validation", "1", "contract-hash", null, null, null, false, null,
            List.of(), PipelineBundleCapabilities.defaults());
        PipelineReleaseIdentityResolver identity = mock(PipelineReleaseIdentityResolver.class);
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        when(identity.contract()).thenReturn(contract);
        when(identity.pipelineId(config)).thenReturn("consumer-validation");
        when(identity.contractVersion()).thenReturn("1");
        when(identity.releaseVersion(config)).thenReturn("1");
        InMemoryPipelineReleaseRegistry registry = new InMemoryPipelineReleaseRegistry();
        LocalPipelineReleaseActivation activation = new LocalPipelineReleaseActivation();
        activation.releaseRegistry = registry;
        activation.releaseIdentity = identity;
        activation.orchestratorConfig = config;

        activation.activate(null);

        PipelineReleaseRecord restored = registry.get("default", "consumer-validation", "1")
            .await().indefinitely().orElseThrow();
        assertEquals(PipelineReleaseStatus.ACTIVE, restored.status());
        assertEquals(contract, restored.contract());
        assertTrue(restored.descriptor().artifacts().isEmpty());
    }

    @Test
    void activatesThroughTheRegistryInterfaceRatherThanItsConcreteImplementation() {
        PipelineContractDescriptor contract = new PipelineContractDescriptor(
            1, "consumer-validation", "1", "contract-hash", null, null, null, false, null,
            List.of(), PipelineBundleCapabilities.defaults());
        PipelineReleaseIdentityResolver identity = mock(PipelineReleaseIdentityResolver.class);
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        when(identity.contract()).thenReturn(contract);
        when(identity.pipelineId(config)).thenReturn("consumer-validation");
        when(identity.contractVersion()).thenReturn("1");
        when(identity.releaseVersion(config)).thenReturn("1");
        PipelineReleaseRegistry registry = mock(PipelineReleaseRegistry.class);
        AtomicReference<PipelineReleaseRecord> registered = new AtomicReference<>();
        when(registry.register(org.mockito.ArgumentMatchers.any())).thenAnswer(invocation -> {
            PipelineReleaseRecord record = invocation.getArgument(0);
            registered.set(record);
            return Uni.createFrom().item(record);
        });
        when(registry.activate(
            org.mockito.ArgumentMatchers.eq("default"),
            org.mockito.ArgumentMatchers.eq("consumer-validation"),
            org.mockito.ArgumentMatchers.eq("1"),
            org.mockito.ArgumentMatchers.anyLong()))
            .thenAnswer(invocation -> Uni.createFrom().item(Optional.of(registered.get())));
        LocalPipelineReleaseActivation activation = new LocalPipelineReleaseActivation();
        activation.releaseRegistry = registry;
        activation.releaseIdentity = identity;
        activation.orchestratorConfig = config;

        activation.activate(null);

        assertEquals(contract, registered.get().contract());
        assertEquals(PipelineReleaseStatus.ACTIVE, registered.get().status());
    }

    @Test
    void activatesTheCurrentReleaseForTheSubmittingTenant() {
        PipelineContractDescriptor contract = new PipelineContractDescriptor(
            1, "restaurant-approval", "1", "contract-hash", null, null, null, false, null,
            List.of(), PipelineBundleCapabilities.defaults());
        PipelineReleaseIdentityResolver identity = mock(PipelineReleaseIdentityResolver.class);
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        when(identity.contract()).thenReturn(contract);
        when(identity.pipelineId(config)).thenReturn("restaurant-approval");
        when(identity.contractVersion()).thenReturn("1");
        when(identity.releaseVersion(config)).thenReturn("1");
        InMemoryPipelineReleaseRegistry registry = new InMemoryPipelineReleaseRegistry();
        LocalPipelineReleaseActivation activation = new LocalPipelineReleaseActivation();
        activation.releaseRegistry = registry;
        activation.releaseIdentity = identity;
        activation.orchestratorConfig = config;

        activation.activateForCurrentRelease("restaurant-demo", "restaurant-approval", "1", "1")
            .await().indefinitely();

        PipelineReleaseRecord restored = registry.get("restaurant-demo", "restaurant-approval", "1")
            .await().indefinitely().orElseThrow();
        assertEquals(PipelineReleaseStatus.ACTIVE, restored.status());
        assertEquals(contract, restored.contract());
    }
}
