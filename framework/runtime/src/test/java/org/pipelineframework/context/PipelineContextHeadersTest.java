package org.pipelineframework.context;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.pipelineframework.runtime.core.TpfContextHeaders;

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
    void frameworkNeutralHeadersDelegateToRuntimeCoreConstants() {
        assertEquals(TpfContextHeaders.VERSION, PipelineContextHeaders.VERSION);
        assertEquals(TpfContextHeaders.REPLAY, PipelineContextHeaders.REPLAY);
        assertEquals(TpfContextHeaders.CACHE_POLICY, PipelineContextHeaders.CACHE_POLICY);
    }

    @Test
    void frameworkNeutralHeadersAreSourcedFromRuntimeCoreConstants() throws Exception {
        String source = Files.readString(
            Path.of("src/main/java/org/pipelineframework/context/PipelineContextHeaders.java"));

        assertTrue(source.contains("public static final String VERSION = TpfContextHeaders.VERSION;"));
        assertTrue(source.contains("public static final String REPLAY = TpfContextHeaders.REPLAY;"));
        assertTrue(source.contains("public static final String CACHE_POLICY = TpfContextHeaders.CACHE_POLICY;"));
    }

    @Test
    void cacheStatusHeaderConstantIsCorrect() {
        assertEquals("x-pipeline-cache-status", PipelineContextHeaders.CACHE_STATUS);
    }

    @Test
    void classHasPrivateConstructor() throws Exception {
        var constructor = PipelineContextHeaders.class.getDeclaredConstructor();
        assertTrue(java.lang.reflect.Modifier.isPrivate(constructor.getModifiers()));
        assertFalse(constructor.canAccess(null));
        constructor.setAccessible(true);
        assertDoesNotThrow(() -> constructor.newInstance());
    }
}
