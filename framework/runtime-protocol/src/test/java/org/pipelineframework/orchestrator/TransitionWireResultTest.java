package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class TransitionWireResultTest {

    @Test
    void completedResultCopiesPortablePayloads() {
        List<SerializedTransitionPayload> payloads = new ArrayList<>();
        payloads.add(new SerializedTransitionPayload("example.Output", "JSON", "{}"));

        TransitionWireResult result = new TransitionWireResult(
            TransitionWorkerOutcome.COMPLETED,
            payloads,
            null,
            null,
            true,
            false);
        payloads.clear();

        assertEquals(1, result.outputPayloads().size());
        assertThrows(UnsupportedOperationException.class, () -> result.outputPayloads().clear());
    }

    @Test
    void rejectsOutcomeDataThatCannotCrossTheWireTogether() {
        TransitionAwaitSuspension suspension = new TransitionAwaitSuspension(
            "tenant-1", "execution-1", "unit-1", 0);
        TransitionFailureEnvelope failure = new TransitionFailureEnvelope(
            IllegalStateException.class.getName(), "failed");

        assertThrows(IllegalArgumentException.class, () -> new TransitionWireResult(
            TransitionWorkerOutcome.WAITING_EXTERNAL,
            List.of(),
            suspension,
            failure,
            false,
            false));
        assertThrows(IllegalArgumentException.class, () -> new TransitionWireResult(
            TransitionWorkerOutcome.FAILED,
            List.of(),
            suspension,
            failure,
            false,
            false));
    }
}
