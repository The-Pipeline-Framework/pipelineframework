package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionRedriveIntent;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerCommand;

class ClaimedSegmentCommandRetryTest {

    @Test
    void carriesFailedRootAndExactLogicalEffectIntoPortableTransition() {
        ExecutionRecord<Object, Object> record = new ExecutionRecord<>(
            "tenant-a",
            "exec-1",
            "key-1",
            "pipeline-a",
            "contract-a",
            "release-a",
            ExecutionResultShape.SINGLE,
            ExecutionStatus.RUNNING,
            8L,
            2,
            3,
            "worker-a",
            100L,
            0L,
            "command-retry:exec-1:7",
            "input",
            null,
            null,
            null,
            null,
            1L,
            2L,
            99L,
            0L,
            0,
            "",
            ExecutionRedriveIntent.RETRY_FAILED_COMMAND,
            5,
            Optional.of("archive:confirmation-7"));

        TransitionCommandEnvelope envelope = ClaimedSegment.from(record)
            .transitionCommand("input", new JsonTransitionPayloadCodec());
        TransitionWorkerCommand decoded = envelope.toCommand(new JsonTransitionPayloadCodec());

        assertEquals(2, envelope.currentStepIndex());
        assertEquals(5, envelope.redriveStepIndex());
        assertEquals(Optional.of("archive:confirmation-7"), envelope.redriveCommandId());
        assertEquals(2, decoded.currentStepIndex());
        assertEquals(5, decoded.redriveStepIndex());
        assertEquals(Optional.of("archive:confirmation-7"), decoded.redriveCommandId());
    }
}
