package org.pipelineframework.awaitable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.JsonDurablePayloadCodec;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;

class AwaitDurablePayloadResolverTest {

    @Test
    void resolvesThePinnedReleaseBindingAndRestoresTheExactCanonicalType() {
        ExecutionStateStore executionStore = mock(ExecutionStateStore.class);
        PipelineReleaseRegistry releases = mock(PipelineReleaseRegistry.class);
        AwaitStepDescriptorFactory descriptors = mock(AwaitStepDescriptorFactory.class);
        @SuppressWarnings("unchecked")
        ExecutionRecord<Object, Object> execution = mock(ExecutionRecord.class);
        when(execution.pipelineId()).thenReturn("payments");
        when(execution.contractVersion()).thenReturn("3");
        when(execution.releaseVersion()).thenReturn("release-7");
        when(executionStore.getExecution("tenant", "execution")).thenReturn(Uni.createFrom().item(Optional.of(execution)));

        PipelineContractDescriptor contract = new PipelineContractDescriptor(
            2, "payments", "3", "contract-hash", null, null, null, false, null,
            java.util.List.of(new PipelineBundleStepDescriptor(2, "branch-after-await", "internal", "ONE_TO_ONE",
                Decision.class.getName(), Decision.class.getName(), null, null, null)),
            PipelineBundleCapabilities.defaults(),
            Map.of(
                "Request", binding(Request.class, "request-fingerprint"),
                "Decision", binding(Decision.class, "decision-fingerprint")),
            "catalog-fingerprint");
        PipelineReleaseRecord release = mock(PipelineReleaseRecord.class);
        when(release.contract()).thenReturn(contract);
        when(releases.get("tenant", "payments", "release-7")).thenReturn(Uni.createFrom().item(Optional.of(release)));

        AwaitDurablePayloadResolver resolver = new AwaitDurablePayloadResolver();
        resolver.executionStateStore = executionStore;
        resolver.releaseRegistry = releases;
        resolver.descriptors = descriptors;
        resolver.codec = new JsonDurablePayloadCodec();
        when(descriptors.descriptorByStepIdNow("await")).thenReturn(descriptor());
        AwaitInteractionRecord interaction = interaction();

        String encodedRequest = resolver.encode(interaction, AwaitDurablePayloadResolver.Slot.REQUEST, new Request("r-1"));
        String encodedResponse = resolver.encode(interaction, AwaitDurablePayloadResolver.Slot.RESPONSE, new Decision("approved"));

        Object restoredRequest = resolver.decode(interaction, AwaitDurablePayloadResolver.Slot.REQUEST, encodedRequest);
        Object restoredResponse = resolver.decode(interaction, AwaitDurablePayloadResolver.Slot.RESPONSE, encodedResponse);

        assertEquals(new Request("r-1"), assertInstanceOf(Request.class, restoredRequest));
        assertEquals(new Decision("approved"), assertInstanceOf(Decision.class, restoredResponse));
        assertTrue(encodedRequest.contains("\"canonicalTypeId\":\"Request\""));
        assertTrue(encodedResponse.contains("\"canonicalTypeId\":\"Decision\""));
    }

    @Test
    void refusesTypedDecodeFailureWithoutFallingBackToAMap() {
        AwaitDurablePayloadResolver resolver = resolverForPinnedRelease();
        String malformed = "{\"canonicalTypeId\":\"Decision\",\"typeExpressionFingerprint\":\"wrong\","
            + "\"catalogFingerprint\":\"catalog-fingerprint\",\"encoding\":\"json\",\"encodingVersion\":1,\"payload\":\"e30=\"}";

        assertThrows(IllegalStateException.class,
            () -> resolver.decode(interaction(), AwaitDurablePayloadResolver.Slot.RESPONSE, malformed));
    }

    @Test
    void legacyClassWrapperRestoresThroughThePinnedCanonicalBinding() {
        AwaitDurablePayloadResolver resolver = resolverForPinnedRelease();
        String legacy = "{\"_tpf_java_class\":\"untrusted.legacy.Decision\",\"_tpf_payload\":{\"status\":\"approved\"}}";

        Object restored = resolver.decodeLegacy(interaction(), AwaitDurablePayloadResolver.Slot.RESPONSE, legacy);

        assertEquals(new Decision("approved"), assertInstanceOf(Decision.class, restored));
    }

    private static AwaitDurablePayloadResolver resolverForPinnedRelease() {
        ExecutionStateStore executionStore = mock(ExecutionStateStore.class);
        PipelineReleaseRegistry releases = mock(PipelineReleaseRegistry.class);
        @SuppressWarnings("unchecked")
        ExecutionRecord<Object, Object> execution = mock(ExecutionRecord.class);
        when(execution.pipelineId()).thenReturn("payments");
        when(execution.contractVersion()).thenReturn("3");
        when(execution.releaseVersion()).thenReturn("release-7");
        when(executionStore.getExecution(any(), any())).thenReturn(Uni.createFrom().item(Optional.of(execution)));
        PipelineContractDescriptor contract = new PipelineContractDescriptor(
            2, "payments", "3", "contract-hash", null, null, null, false, null,
            java.util.List.of(new PipelineBundleStepDescriptor(2, "await", "await", "ONE_TO_ONE",
                "Request", "Decision", null, null, "kafka")),
            PipelineBundleCapabilities.defaults(),
            Map.of("Decision", binding(Decision.class, "decision-fingerprint"), "Request", binding(Request.class, "request-fingerprint")),
            "catalog-fingerprint");
        PipelineReleaseRecord release = mock(PipelineReleaseRecord.class);
        when(release.contract()).thenReturn(contract);
        when(releases.get(any(), any(), any())).thenReturn(Uni.createFrom().item(Optional.of(release)));
        AwaitDurablePayloadResolver resolver = new AwaitDurablePayloadResolver();
        resolver.executionStateStore = executionStore;
        resolver.releaseRegistry = releases;
        resolver.descriptors = mock(AwaitStepDescriptorFactory.class);
        when(resolver.descriptors.descriptorByStepIdNow("await")).thenReturn(descriptor());
        resolver.codec = new JsonDurablePayloadCodec();
        return resolver;
    }

    private static Map<String, Object> binding(Class<?> type, String fingerprint) {
        return Map.of("runtimeClass", type.getName(), "definitionFingerprint", fingerprint);
    }

    private static AwaitInteractionRecord interaction() {
        return new AwaitInteractionRecord("tenant", "execution", "await", 2, Decision.class.getName(), "interaction",
            "correlation", null, "idem", 0, AwaitInteractionStatus.WAITING, null, null, "unit", 0, null, null,
            null, "kafka", Map.of(), 100_000L, 1L, 1L, 1_000L, Decision.class.getName());
    }

    private static AwaitStepDescriptor descriptor() {
        return new AwaitStepDescriptor("await", Request.class.getName(), Decision.class.getName(),
            "ONE_TO_ONE", Duration.ofSeconds(30), "correlation", "kafka", Map.of(), java.util.List.of());
    }

    private record Request(String id) { }
    private record Decision(String status) { }
}
