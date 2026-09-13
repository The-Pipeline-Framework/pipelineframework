package org.pipelineframework.execution;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineExecutionContextTest {

    @Test
    void recordShapeRemainsTheDurableExecutionIdentitySurface() {
        assertEquals(
            List.of(
                "tenantId",
                "executionId",
                "pipelineId",
                "contractVersion",
                "releaseVersion",
                "currentStepIndex",
                "correlationId",
                "traceId"),
            Arrays.stream(PipelineExecutionContext.class.getRecordComponents())
                .map(RecordComponent::getName)
                .toList());
    }

    @Test
    void advancingAStepPreservesPinnedExecutionIdentity() {
        PipelineExecutionContext context = new PipelineExecutionContext(
            "tenant-1",
            "execution-1",
            "pipeline-1",
            "contract-3",
            "release-9",
            2,
            Optional.of("correlation-5"),
            Optional.of("trace-7"));

        PipelineExecutionContext advanced = context.atStep(3);

        assertEquals("tenant-1", advanced.tenantId());
        assertEquals("execution-1", advanced.executionId());
        assertEquals("pipeline-1", advanced.pipelineId());
        assertEquals("contract-3", advanced.contractVersion());
        assertEquals("release-9", advanced.releaseVersion());
        assertEquals(Optional.of("correlation-5"), advanced.correlationId());
        assertEquals(Optional.of("trace-7"), advanced.traceId());
        assertEquals(3, advanced.currentStepIndex());
    }

    @Test
    void rejectsUnusableDurableIdentity() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new PipelineExecutionContext(" ", "execution-1", 0));
        assertThrows(
            IllegalArgumentException.class,
            () -> new PipelineExecutionContext("tenant-1", "execution-1", -1));
    }
}
