package org.pipelineframework.context;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class PipelineContextTest {

    @Test
    void recordShapeRemainsThePropagationCompatibilitySurface() {
        assertEquals(
            List.of("versionTag", "replayMode", "cachePolicy"),
            Arrays.stream(PipelineContext.class.getRecordComponents())
                .map(RecordComponent::getName)
                .toList());
    }

    @Test
    void headerFactoryPreservesAuthoredNormalization() {
        PipelineContext context = PipelineContext.fromHeaders(" release-7 ", " ", " require-cache ");

        assertEquals("release-7", context.versionTag());
        assertNull(context.replayMode());
        assertEquals("require-cache", context.cachePolicy());
    }
}
