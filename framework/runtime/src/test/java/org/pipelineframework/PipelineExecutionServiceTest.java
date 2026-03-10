package org.pipelineframework;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PipelineExecutionServiceTest {

    private PipelineExecutionService service;

    @Mock
    private QueueAsyncCoordinator queueAsyncCoordinator;

    @BeforeEach
    void setUp() {
        service = new PipelineExecutionService();
        service.queueAsyncCoordinator = queueAsyncCoordinator;
    }

    @Test
    void startupHealthStateInitiallyPending() {
        assertEquals(PipelineExecutionService.StartupHealthState.PENDING, service.getStartupHealthState());
    }

    @Test
    void executePipelineAsyncDefaultsToUnaryOutputMode() {
        RunAsyncAcceptedDto expected = new RunAsyncAcceptedDto("exec-1", false, "/pipeline/executions/exec-1", 1L);
        when(queueAsyncCoordinator.executePipelineAsync("input", "tenant-1", "idem-1", false))
            .thenReturn(Uni.createFrom().item(expected));

        RunAsyncAcceptedDto actual = service.executePipelineAsync("input", "tenant-1", "idem-1")
            .await().indefinitely();

        assertEquals(expected.executionId(), actual.executionId());
        verify(queueAsyncCoordinator).executePipelineAsync("input", "tenant-1", "idem-1", false);
    }

    @Test
    void executePipelineAsyncPassesOutputStreamingFlag() {
        RunAsyncAcceptedDto expected = new RunAsyncAcceptedDto("exec-2", false, "/pipeline/executions/exec-2", 2L);
        when(queueAsyncCoordinator.executePipelineAsync("input", "tenant-1", "idem-1", true))
            .thenReturn(Uni.createFrom().item(expected));

        RunAsyncAcceptedDto actual = service.executePipelineAsync("input", "tenant-1", "idem-1", true)
            .await().indefinitely();

        assertEquals(expected.executionId(), actual.executionId());
        verify(queueAsyncCoordinator).executePipelineAsync("input", "tenant-1", "idem-1", true);
    }

    @Test
    void getExecutionStatusDelegatesToCoordinator() {
        ExecutionStatusDto expected = new ExecutionStatusDto("exec-3", null, 0, 0, 1L, 0L, 0L, null, null);
        when(queueAsyncCoordinator.getExecutionStatus("tenant-1", "exec-3"))
            .thenReturn(Uni.createFrom().item(expected));

        ExecutionStatusDto actual = service.getExecutionStatus("tenant-1", "exec-3").await().indefinitely();

        assertEquals("exec-3", actual.executionId());
        verify(queueAsyncCoordinator).getExecutionStatus("tenant-1", "exec-3");
    }

    @Test
    void processExecutionWorkItemDelegatesToCoordinator() {
        ExecutionWorkItem item = new ExecutionWorkItem("tenant-1", "exec-4");
        when(queueAsyncCoordinator.processExecutionWorkItem(eq(item), org.mockito.ArgumentMatchers.any()))
            .thenReturn(Uni.createFrom().voidItem());

        service.processExecutionWorkItem(item).await().indefinitely();

        verify(queueAsyncCoordinator).processExecutionWorkItem(eq(item), org.mockito.ArgumentMatchers.any());
    }
}
