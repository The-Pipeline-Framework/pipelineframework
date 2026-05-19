package org.pipelineframework.awaitable;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AwaitStepSupportTest {

    @Mock
    PipelineOrchestratorConfig orchestratorConfig;

    @Mock
    AwaitCoordinator awaitCoordinator;

    @AfterEach
    void clearContext() {
        AwaitExecutionContextHolder.clear();
    }

    @Test
    void awaitOneToOneFailsOutsideQueueAsyncMode() {
        AwaitStepSupport support = support();
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.SYNC);

        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> support.awaitOneToOne(descriptor(), "input").await().indefinitely());

        assertTrue(error.getMessage().contains("pipeline.orchestrator.mode=QUEUE_ASYNC"));
    }

    @Test
    void awaitOneToOneFailsWithoutExecutionContext() {
        AwaitStepSupport support = support();
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);

        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> support.awaitOneToOne(descriptor(), "input").await().indefinitely());

        assertTrue(error.getMessage().contains("without queue-async execution context"));
    }

    private AwaitStepSupport support() {
        AwaitStepSupport support = new AwaitStepSupport();
        support.orchestratorConfig = orchestratorConfig;
        support.awaitCoordinator = awaitCoordinator;
        return support;
    }

    private AwaitStepDescriptor descriptor() {
        return new AwaitStepDescriptor(
            "review",
            String.class.getName(),
            String.class.getName(),
            Duration.ofMinutes(5),
            "interactionId",
            "interaction-api",
            Map.of(),
            List.of());
    }
}
