package org.pipelineframework.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.pipelineframework.command.CommandEffectStoreConfig;
import org.pipelineframework.query.QueryCaptureStoreConfig;

class PipelineFrameworkProcessorTest {
    private final PipelineFrameworkProcessor processor = new PipelineFrameworkProcessor();

    @Test
    void registersQueryCaptureConfigurationIndependentlyOfTheSelectedStoreBean() {
        var mapping = processor.queryCaptureStoreConfig();

        assertEquals(QueryCaptureStoreConfig.class, mapping.getConfigClass());
        assertEquals("pipeline.query.capture-store", mapping.getPrefix());
    }

    @Test
    void registersCommandEffectConfigurationIndependentlyOfTheSelectedStoreBean() {
        var mapping = processor.commandEffectStoreConfig();

        assertEquals(CommandEffectStoreConfig.class, mapping.getConfigClass());
        assertEquals("pipeline.command.effect-store", mapping.getPrefix());
    }
}
