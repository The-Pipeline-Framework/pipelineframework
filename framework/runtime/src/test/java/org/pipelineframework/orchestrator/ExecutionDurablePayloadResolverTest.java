package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;

import io.smallrye.mutiny.Uni;

class ExecutionDurablePayloadResolverTest {

    @Test
    void restoresCanonicalInputAndItemizedChildResultsFromAPinnedRelease() {
        ExecutionDurablePayloadResolver resolver = resolver();
        ExecutionRecord<Object, Object> inputExecution = execution(0);
        ExecutionRecord<Object, Object> resultExecution = execution(1);
        PaymentRecord input = new PaymentRecord("payment-1");
        List<PaymentOutput> children = List.of(new PaymentOutput("payment-1", "approved"));

        String encodedInput = resolver.encode(inputExecution, ExecutionDurablePayloadResolver.Slot.INPUT, input);
        String encodedChildren = resolver.encode(resultExecution, ExecutionDurablePayloadResolver.Slot.RESULT, children);

        assertEquals(input, assertInstanceOf(PaymentRecord.class,
            resolver.decode(inputExecution, ExecutionDurablePayloadResolver.Slot.INPUT, encodedInput)));
        Object restored = resolver.decode(resultExecution, ExecutionDurablePayloadResolver.Slot.RESULT, encodedChildren);
        List<?> restoredChildren = assertInstanceOf(List.class, restored);
        assertEquals(children, restoredChildren);
        assertInstanceOf(PaymentOutput.class, restoredChildren.getFirst());
    }

    @Test
    void refusesTypedExecutionPayloadMismatchWithoutMapFallback() {
        ExecutionDurablePayloadResolver resolver = resolver();
        String invalid = "{\"canonicalTypeId\":\"PaymentOutput\",\"typeExpressionFingerprint\":\"wrong\","
            + "\"catalogFingerprint\":\"catalog\",\"encoding\":\"application/tpf-canonical+json\",\"encodingVersion\":1,\"payload\":\"e30=\"}";

        assertThrows(IllegalStateException.class,
            () -> resolver.decode(execution(1), ExecutionDurablePayloadResolver.Slot.RESULT, invalid));
    }

    @Test
    void legacyExecutionPayloadRestoresThroughThePinnedBinding() {
        PaymentOutput expected = new PaymentOutput("payment-1", "approved");

        Object restored = resolver().decodeLegacy(execution(1), ExecutionDurablePayloadResolver.Slot.RESULT,
            "{\"_tpf_java_class\":\"untrusted.LegacyOutput\",\"_tpf_payload\":{\"id\":\"payment-1\",\"status\":\"approved\"}}");

        assertEquals(expected, assertInstanceOf(PaymentOutput.class, restored));
    }

    private static ExecutionDurablePayloadResolver resolver() {
        PipelineReleaseRegistry registry = mock(PipelineReleaseRegistry.class);
        PipelineContractDescriptor contract = new PipelineContractDescriptor(2, "payments", "3", "contract", null, null,
            null, false, null, List.of(
                new PipelineBundleStepDescriptor(0, "input", "object", "ONE_TO_ONE", "PaymentRecord", "PaymentRecord", null, null, null),
                new PipelineBundleStepDescriptor(1, "terminal", "terminal", "ONE_TO_ONE", "PaymentOutput", "PaymentOutput", null, null, null)),
            PipelineBundleCapabilities.defaults(), Map.of(
                "PaymentRecord", binding(PaymentRecord.class, "record-fingerprint"),
                "PaymentOutput", binding(PaymentOutput.class, "output-fingerprint")), "catalog");
        PipelineReleaseRecord release = mock(PipelineReleaseRecord.class);
        when(release.contract()).thenReturn(contract);
        when(registry.get("tenant", "payments", "release")).thenReturn(Uni.createFrom().item(Optional.of(release)));
        ExecutionDurablePayloadResolver resolver = new ExecutionDurablePayloadResolver();
        resolver.releaseRegistry = registry;
        resolver.codec = new JsonDurablePayloadCodec();
        return resolver;
    }

    @SuppressWarnings("unchecked")
    private static ExecutionRecord<Object, Object> execution(int stepIndex) {
        ExecutionRecord<Object, Object> execution = mock(ExecutionRecord.class);
        when(execution.tenantId()).thenReturn("tenant");
        when(execution.pipelineId()).thenReturn("payments");
        when(execution.contractVersion()).thenReturn("3");
        when(execution.releaseVersion()).thenReturn("release");
        when(execution.executionId()).thenReturn("execution");
        when(execution.currentStepIndex()).thenReturn(stepIndex);
        when(execution.resultShape()).thenReturn(ExecutionResultShape.SINGLE);
        return execution;
    }

    private static Map<String, Object> binding(Class<?> type, String fingerprint) {
        return Map.of("runtimeClass", type.getName(), "definitionFingerprint", fingerprint);
    }

    private record PaymentRecord(String id) { }
    private record PaymentOutput(String id, String status) { }
}
