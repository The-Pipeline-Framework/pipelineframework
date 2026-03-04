package org.pipelineframework.context;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PipelineContextHeadersTest {

    @Test
    void versionHeaderConstantIsCorrect() {
        assertEquals("x-pipeline-version", PipelineContextHeaders.VERSION);
    }

    @Test
    void replayHeaderConstantIsCorrect() {
        assertEquals("x-pipeline-replay", PipelineContextHeaders.REPLAY);
    }

    @Test
    void cachePolicyHeaderConstantIsCorrect() {
        assertEquals("x-pipeline-cache-policy", PipelineContextHeaders.CACHE_POLICY);
    }

    @Test
    void cacheStatusHeaderConstantIsCorrect() {
        assertEquals("x-pipeline-cache-status", PipelineContextHeaders.CACHE_STATUS);
    }

    @Test
    void classHasPrivateConstructor() throws Exception {
        var constructor = PipelineContextHeaders.class.getDeclaredConstructor();
        assertTrue(java.lang.reflect.Modifier.isPrivate(constructor.getModifiers()));
        constructor.setAccessible(true);
        assertDoesNotThrow(() -> constructor.newInstance());
    }
}
