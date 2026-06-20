package org.pipelineframework.invocation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.runtime.core.RuntimeAdapters;

class InvocationContextSnapshotTest {

    @BeforeEach
    void resetContext() {
        RuntimeAdapters.resetForTests();
        PipelineContextHolder.clear();
        AwaitExecutionContextHolder.clear();
        PipelineExecutionContextHolder.clear();
    }

    @AfterEach
    void cleanupContext() {
        RuntimeAdapters.resetForTests();
        PipelineContextHolder.clear();
        AwaitExecutionContextHolder.clear();
        PipelineExecutionContextHolder.clear();
    }

    @Test
    void installsAwaitContextAndDerivesPipelineExecutionContext() {
        AwaitExecutionContext awaitCtx = new AwaitExecutionContext("tenant-1", "exec-abc", 3);
        InvocationContextSnapshot snapshot = new InvocationContextSnapshot(null, awaitCtx);

        snapshot.run(() -> {
            AwaitExecutionContext installed = AwaitExecutionContextHolder.get();
            assertEquals("tenant-1", installed.tenantId());
            assertEquals("exec-abc", installed.executionId());
            assertEquals(3, installed.currentStepIndex());

            PipelineExecutionContext executionCtx = PipelineExecutionContextHolder.get();
            assertEquals("tenant-1", executionCtx.tenantId());
            assertEquals("exec-abc", executionCtx.executionId());
            assertEquals(3, executionCtx.currentStepIndex());
        });
    }

    @Test
    void clearsAwaitContextAndPipelineExecutionContextWhenAwaitContextIsNull() {
        AwaitExecutionContext awaitCtx = new AwaitExecutionContext("tenant-1", "exec-xyz", 5);
        AwaitExecutionContextHolder.set(awaitCtx);
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant-1", "exec-xyz", 5));

        InvocationContextSnapshot snapshot = new InvocationContextSnapshot(null, null);

        snapshot.run(() -> {
            assertNull(AwaitExecutionContextHolder.get());
            assertNull(PipelineExecutionContextHolder.get());
        });
    }

    @Test
    void restoresPreviousAwaitContextAndPipelineExecutionContextAfterScope() {
        AwaitExecutionContext previous = new AwaitExecutionContext("tenant-prev", "exec-prev", 1);
        AwaitExecutionContextHolder.set(previous);
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant-prev", "exec-prev", 1));

        AwaitExecutionContext awaitCtx = new AwaitExecutionContext("tenant-new", "exec-new", 7);
        InvocationContextSnapshot snapshot = new InvocationContextSnapshot(null, awaitCtx);

        snapshot.run(() -> {
            assertEquals("tenant-new", AwaitExecutionContextHolder.get().tenantId());
            assertEquals("tenant-new", PipelineExecutionContextHolder.get().tenantId());
        });

        // After scope closes, previous contexts are restored
        assertEquals("tenant-prev", AwaitExecutionContextHolder.get().tenantId());
        assertEquals("tenant-prev", PipelineExecutionContextHolder.get().tenantId());
    }

    @Test
    void restoresPreviousContextsWhenScopedAwaitContextIsNull() {
        AwaitExecutionContext previous = new AwaitExecutionContext("tenant-original", "exec-original", 2);
        AwaitExecutionContextHolder.set(previous);
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant-original", "exec-original", 2));

        InvocationContextSnapshot snapshot = new InvocationContextSnapshot(null, null);

        snapshot.run(() -> {
            assertNull(AwaitExecutionContextHolder.get());
            assertNull(PipelineExecutionContextHolder.get());
        });

        // After scope, previous contexts are restored
        assertEquals("tenant-original", AwaitExecutionContextHolder.get().tenantId());
        assertEquals("tenant-original", PipelineExecutionContextHolder.get().tenantId());
    }

    @Test
    void pipelineExecutionContextStepIndexMatchesAwaitContextStepIndex() {
        AwaitExecutionContext awaitCtx = new AwaitExecutionContext("tenant-1", "exec-abc", 10);
        InvocationContextSnapshot snapshot = new InvocationContextSnapshot(null, awaitCtx);

        snapshot.run(() -> {
            PipelineExecutionContext executionCtx = PipelineExecutionContextHolder.get();
            assertEquals(awaitCtx.currentStepIndex(), executionCtx.currentStepIndex());
        });
    }

    @Test
    void pipelineContextIsInstalledWhenProvided() {
        PipelineContext pipelineCtx = new PipelineContext("v1", null, null);
        InvocationContextSnapshot snapshot = new InvocationContextSnapshot(pipelineCtx, null);

        snapshot.run(() -> {
            assertEquals(pipelineCtx, PipelineContextHolder.get());
        });
    }
}
