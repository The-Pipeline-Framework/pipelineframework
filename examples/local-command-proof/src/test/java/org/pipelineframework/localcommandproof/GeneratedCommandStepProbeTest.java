package org.pipelineframework.localcommandproof;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.command.CommandEffectStatus;
import org.pipelineframework.command.InMemoryCommandEffectStore;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.pipeline.service.pipeline.ProcessObservedOperationCommandClientStep;

@QuarkusTest
class GeneratedCommandStepProbeTest {

    @Inject
    ProcessObservedOperationCommandClientStep step;

    @Inject
    InMemoryCommandEffectStore commandEffects;

    @Test
    void generatedCommandStepCompletesBelowTheQueueAsyncTransitionBoundary() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext(
            "probe-tenant", "probe-execution", 0));
        ObservedOperationCommand input = new ObservedOperationCommand(
            "probe-success", ObservedOperationCommand.Behavior.SUCCESS);

        ObservedOperationResult result = step.applyOneToOne(input).await().atMost(Duration.ofSeconds(5));

        assertEquals("probe-success", result.operationId());
        assertEquals(
            CommandEffectStatus.SUCCEEDED,
            commandEffects.find("probe-tenant", "observed-operation:probe-success")
                .await().atMost(Duration.ofSeconds(5)).orElseThrow().status());
    }

    @AfterEach
    void clearContext() {
        PipelineExecutionContextHolder.clear();
    }
}
