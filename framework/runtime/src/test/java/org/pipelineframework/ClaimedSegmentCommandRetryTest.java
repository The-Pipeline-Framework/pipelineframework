package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionRedriveIntent;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerCommand;

class ClaimedSegmentCommandRetryTest {

    @ParameterizedTest
    @ValueSource(ints = {2, 5, 6})
    void scopesRetryAuthorityToSegmentsBeforeOrAtTheFailedRoot(int stepIndex) {
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
            stepIndex,
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

        boolean completed = stepIndex > 5;
        assertEquals(stepIndex, envelope.currentStepIndex());
        assertEquals(completed ? ExecutionRedriveIntent.REPLAY : ExecutionRedriveIntent.RETRY_FAILED_COMMAND,
            envelope.redriveIntent());
        assertEquals(completed ? -1 : 5, envelope.redriveStepIndex());
        assertEquals(completed ? Optional.empty() : Optional.of("archive:confirmation-7"), envelope.redriveCommandId());
        assertEquals(stepIndex, decoded.currentStepIndex());
        assertEquals(envelope.redriveIntent(), decoded.redriveIntent());
        assertEquals(envelope.redriveStepIndex(), decoded.redriveStepIndex());
        assertEquals(envelope.redriveCommandId(), decoded.redriveCommandId());
        assertEquals(ExecutionRedriveIntent.RETRY_FAILED_COMMAND, record.redriveIntent(), "preserve persisted audit history");
    }
}
