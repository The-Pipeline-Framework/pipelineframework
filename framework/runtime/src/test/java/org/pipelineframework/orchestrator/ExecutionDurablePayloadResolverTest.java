package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
        ExecutionRecord<Object, Object> resultExecution = execution(5, ExecutionResultShape.MATERIALIZED_MULTI);
        CsvPaymentsInputFile input = new CsvPaymentsInputFile("/app/test-e2e/payments.csv");
        List<PaymentOutput> children = List.of(new PaymentOutput("payment-1", "approved"));

        String encodedInput = resolver.encode(inputExecution, ExecutionDurablePayloadResolver.Slot.INPUT, input);
        String encodedChildren = resolver.encode(resultExecution, ExecutionDurablePayloadResolver.Slot.RESULT, children);

        assertEquals(input, assertInstanceOf(CsvPaymentsInputFile.class,
            resolver.decode(inputExecution, ExecutionDurablePayloadResolver.Slot.INPUT, encodedInput)));
        Object restored = resolver.decode(resultExecution, ExecutionDurablePayloadResolver.Slot.RESULT, encodedChildren);
        List<?> restoredChildren = assertInstanceOf(List.class, restored);
        assertEquals(children, restoredChildren);
        assertInstanceOf(PaymentOutput.class, restoredChildren.getFirst());
    }

    @Test
    void restoresTypedEntryInputAfterTheExecutionHasAdvancedToAnAwaitContinuation() {
        ExecutionDurablePayloadResolver resolver = resolver();
        CsvPaymentsInputFile original = new CsvPaymentsInputFile("/app/test-e2e/payments.csv");

        String encoded = resolver.encode(execution(0), ExecutionDurablePayloadResolver.Slot.INPUT, original);

        assertEquals(original, assertInstanceOf(CsvPaymentsInputFile.class,
            resolver.decode(execution(2), ExecutionDurablePayloadResolver.Slot.INPUT, encoded)));
    }

    @Test
    void restoresTypedContinuationInputFromItsStoredIdentity() {
        ExecutionDurablePayloadResolver resolver = resolver();
        PaymentStatus continuation = new PaymentStatus("payment-1", "approved");

        String encoded = resolver.encode(execution(2), ExecutionDurablePayloadResolver.Slot.CONTINUATION_INPUT, continuation);

        assertEquals(continuation, assertInstanceOf(PaymentStatus.class,
            resolver.decode(execution(2), ExecutionDurablePayloadResolver.Slot.INPUT, encoded)));
    }

    @Test
    void encodesABranchContinuationUsingTheReceivingStepInputNotThePreviousBranchOutput() {
        ExecutionDurablePayloadResolver resolver = resolver();
        PaymentStatus continuation = new PaymentStatus("payment-1", "approved");

        String encoded = resolver.encode(execution(4), ExecutionDurablePayloadResolver.Slot.CONTINUATION_INPUT, continuation);

        assertTrue(encoded.contains("\"canonicalTypeId\":\"PaymentStatus\""));
        assertEquals(continuation, assertInstanceOf(PaymentStatus.class,
            resolver.decode(execution(4), ExecutionDurablePayloadResolver.Slot.INPUT, encoded)));
    }

    @Test
    void encodesAnOutOfRangeGeneratedContinuationCursorAsTheTerminalSemanticOutput() {
        ExecutionDurablePayloadResolver resolver = resolver();
        List<PaymentOutput> outputs = List.of(new PaymentOutput("payment-1", "approved"));

        String encoded = resolver.encode(execution(8), ExecutionDurablePayloadResolver.Slot.CONTINUATION_INPUT, outputs);

        assertTrue(encoded.contains("\"canonicalTypeId\":\"List<PaymentOutput>\""));
        List<?> restored = assertInstanceOf(List.class,
            resolver.decode(execution(8), ExecutionDurablePayloadResolver.Slot.INPUT, encoded));
        assertInstanceOf(PaymentOutput.class, restored.getFirst());
    }

    @Test
    void rejectsCatalogTypeThatIsNotAnAllowedExecutionInputOccupant() {
        ExecutionDurablePayloadResolver resolver = resolver();
        String encoded = "{\"canonicalTypeId\":\"Unrelated\",\"typeExpressionFingerprint\":\"unrelated-fingerprint\","
            + "\"catalogFingerprint\":\"catalog\",\"encoding\":\"application/tpf-canonical+json\",\"encodingVersion\":1,\"payload\":\"e30=\"}";

        IllegalStateException failure = assertThrows(IllegalStateException.class,
            () -> resolver.decode(execution(2), ExecutionDurablePayloadResolver.Slot.INPUT, encoded));

        assertTrue(failure.getCause().getMessage().contains("not permitted for INPUT"));
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

        Object restored = resolver().decodeLegacy(execution(5), ExecutionDurablePayloadResolver.Slot.RESULT,
            "{\"_tpf_java_class\":\"untrusted.LegacyOutput\",\"_tpf_payload\":{\"id\":\"payment-1\",\"status\":\"approved\"}}");

        assertEquals(expected, assertInstanceOf(PaymentOutput.class, restored));
    }

    @Test
    void legacyInputStillUsesTheExplicitCompatibilityExpression() {
        CsvPaymentsInputFile expected = new CsvPaymentsInputFile("/app/test-e2e/payments.csv");

        Object restored = resolver().decodeLegacy(execution(0), ExecutionDurablePayloadResolver.Slot.INPUT,
            "{\"_tpf_payload\":{\"path\":\"/app/test-e2e/payments.csv\"}}");

        assertEquals(expected, assertInstanceOf(CsvPaymentsInputFile.class, restored));
    }

    private static ExecutionDurablePayloadResolver resolver() {
        PipelineReleaseRegistry registry = mock(PipelineReleaseRegistry.class);
        PipelineContractDescriptor contract = new PipelineContractDescriptor(2, "payments", "3", "contract", null, null,
            null, false, null, List.of(
                new PipelineBundleStepDescriptor(0, "input", "object", "ONE_TO_ONE", CsvPaymentsInputFile.class.getName(), PaymentRecord.class.getName(), null, null, null),
                new PipelineBundleStepDescriptor(1, "await", "await", "ONE_TO_ONE", PaymentRecord.class.getName(), PaymentStatus.class.getName(), null, null, null),
                new PipelineBundleStepDescriptor(2, "terminal", "terminal", "ONE_TO_ONE", PaymentStatus.class.getName(), PaymentOutput.class.getName(), null, null, null),
                new PipelineBundleStepDescriptor(3, "approved", "branch", "ONE_TO_ONE", PaymentStatus.class.getName(), ApprovedPaymentOutput.class.getName(), null, null, null),
                new PipelineBundleStepDescriptor(4, "unapproved", "branch", "ONE_TO_ONE", PaymentStatus.class.getName(), PaymentOutput.class.getName(), null, null, null)),
            PipelineBundleCapabilities.defaults(), Map.of(
                "CsvPaymentsInputFile", binding(CsvPaymentsInputFile.class, "input-file-fingerprint"),
                "PaymentRecord", binding(PaymentRecord.class, "record-fingerprint"),
                "PaymentStatus", binding(PaymentStatus.class, "status-fingerprint"),
                "ApprovedPaymentOutput", binding(ApprovedPaymentOutput.class, "approved-output-fingerprint"),
                "PaymentOutput", binding(PaymentOutput.class, "output-fingerprint"),
                "Unrelated", binding(Unrelated.class, "unrelated-fingerprint")), "catalog");
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
        return execution(stepIndex, ExecutionResultShape.SINGLE);
    }

    @SuppressWarnings("unchecked")
    private static ExecutionRecord<Object, Object> execution(int stepIndex, ExecutionResultShape resultShape) {
        ExecutionRecord<Object, Object> execution = mock(ExecutionRecord.class);
        when(execution.tenantId()).thenReturn("tenant");
        when(execution.pipelineId()).thenReturn("payments");
        when(execution.contractVersion()).thenReturn("3");
        when(execution.releaseVersion()).thenReturn("release");
        when(execution.executionId()).thenReturn("execution");
        when(execution.currentStepIndex()).thenReturn(stepIndex);
        when(execution.resultShape()).thenReturn(resultShape);
        return execution;
    }

    private static Map<String, Object> binding(Class<?> type, String fingerprint) {
        return Map.of("runtimeClass", type.getName(), "definitionFingerprint", fingerprint);
    }

    private record CsvPaymentsInputFile(String path) { }
    private record PaymentRecord(String id) { }
    private record PaymentStatus(String id, String status) { }
    private record ApprovedPaymentOutput(String id, String status) { }
    private record PaymentOutput(String id, String status) { }
    private record Unrelated(String value) { }
}
