package org.pipelineframework.orchestrator.controlplane;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.junit.jupiter.api.Test;

class BoundaryAdmissionFactsTest {

    @Test
    void awaitKafkaCompletionAndCheckpointKafkaHandoffUseSameCompletionFactShape() {
        ControlPlaneFact awaitCompletion = BoundaryAdmissionFacts.completion(new BoundaryAdmissionRequest(
            "tenant",
            "run-1",
            "await-unit",
            BoundaryKind.AWAIT,
            "await-interaction",
            "await-idem",
            "payment-status"));
        ControlPlaneFact checkpointCompletion = BoundaryAdmissionFacts.completion(new BoundaryAdmissionRequest(
            "tenant",
            "run-1",
            "checkpoint-unit",
            BoundaryKind.CHECKPOINT,
            "checkpoint-interaction",
            "checkpoint-idem",
            "handoff-payload"));

        assertInstanceOf(ControlPlaneFact.BoundaryCompletionAdmitted.class, awaitCompletion);
        assertInstanceOf(ControlPlaneFact.BoundaryCompletionAdmitted.class, checkpointCompletion);
        assertEquals("boundary-completion-admitted:await-unit:await-idem", awaitCompletion.factKey());
        assertEquals("boundary-completion-admitted:checkpoint-unit:checkpoint-idem", checkpointCompletion.factKey());
    }
}
