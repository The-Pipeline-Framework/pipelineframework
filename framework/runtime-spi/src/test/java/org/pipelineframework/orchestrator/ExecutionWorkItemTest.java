package org.pipelineframework.orchestrator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ExecutionWorkItemTest {

    @Test
    void rejectsNullTenantId() {
        NullPointerException failure =
            assertThrows(NullPointerException.class, () -> new ExecutionWorkItem(null, "exec-1"));
        assertEquals("ExecutionWorkItem.tenantId must not be null", failure.getMessage());
    }

    @Test
    void rejectsNullExecutionId() {
        NullPointerException failure =
            assertThrows(NullPointerException.class, () -> new ExecutionWorkItem("tenant-a", null));
        assertEquals("ExecutionWorkItem.executionId must not be null", failure.getMessage());
    }
}
