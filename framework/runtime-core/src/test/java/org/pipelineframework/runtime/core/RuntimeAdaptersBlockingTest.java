package org.pipelineframework.runtime.core;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class RuntimeAdaptersBlockingTest {
    @AfterEach
    void reset() {
        RuntimeAdapters.resetForTests();
    }

    @Test
    void fallbackBlockingBoundaryUsesAWorker() throws Exception {
        String caller = Thread.currentThread().getName();
        RuntimeAdapters.setExecutionContext("tenant", "tenant-1");

        Result result = RuntimeAdapters.executeBlocking(
            () -> new Result(
                Thread.currentThread().getName(),
                RuntimeAdapters.executionContext("tenant", String.class)),
            false).toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertNotEquals(caller, result.thread());
        assertEquals("tenant-1", result.tenant());
    }

    private record Result(String thread, String tenant) {
    }
}
