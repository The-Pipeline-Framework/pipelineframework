package org.pipelineframework.config.composition;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class PipelineCompositionSchemaResourceTest {

    @Test
    void compositionSchemaResourceIsPackaged() {
        assertNotNull(Thread.currentThread().getContextClassLoader()
            .getResource("META-INF/pipeline/pipeline-composition-schema.json"));
    }
}
