package org.pipelineframework.awaitable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class AwaitExecutionContextHolderTest {

    @AfterEach
    void cleanup() {
        AwaitExecutionContextHolder.clear();
    }

    @Test
    void returnsNullWhenNothingSet() {
        assertNull(AwaitExecutionContextHolder.get());
    }

    @Test
    void setsAndGetsContext() {
        AwaitExecutionContext context = new AwaitExecutionContext("tenant1", "exec-1", 0);
        AwaitExecutionContextHolder.set(context);

        assertSame(context, AwaitExecutionContextHolder.get());
    }

    @Test
    void clearRemovesContext() {
        AwaitExecutionContext context = new AwaitExecutionContext("tenant1", "exec-1", 0);
        AwaitExecutionContextHolder.set(context);
        AwaitExecutionContextHolder.clear();

        assertNull(AwaitExecutionContextHolder.get());
    }

    @Test
    void overwritesExistingContext() {
        AwaitExecutionContext first = new AwaitExecutionContext("tenant1", "exec-1", 0);
        AwaitExecutionContext second = new AwaitExecutionContext("tenant2", "exec-2", 3);
        AwaitExecutionContextHolder.set(first);
        AwaitExecutionContextHolder.set(second);

        assertSame(second, AwaitExecutionContextHolder.get());
        assertEquals("tenant2", AwaitExecutionContextHolder.get().tenantId());
    }

    @Test
    void clearIsIdempotent() {
        AwaitExecutionContextHolder.clear();
        AwaitExecutionContextHolder.clear();

        assertNull(AwaitExecutionContextHolder.get());
    }

    @Test
    void threadLocalIsolation() throws InterruptedException {
        AwaitExecutionContext mainContext = new AwaitExecutionContext("main-tenant", "main-exec", 0);
        AwaitExecutionContextHolder.set(mainContext);

        AwaitExecutionContext[] capturedInThread = new AwaitExecutionContext[1];
        Thread thread = new Thread(() -> capturedInThread[0] = AwaitExecutionContextHolder.get());
        thread.start();
        thread.join();

        // Other thread should see null since context is thread-local
        assertNull(capturedInThread[0]);
        // Main thread should still have its context
        assertSame(mainContext, AwaitExecutionContextHolder.get());
    }
}