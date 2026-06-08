package org.pipelineframework.processor.renderer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FunctionHandlerRendererFactoryTest {

    @Test
    void createRendererResolvesProfileDefaults() {
        assertDoesNotThrow(() -> {
            AbstractFunctionHandlerRenderer renderer = FunctionHandlerRendererFactory.createRenderer(null);
            assertNotNull(renderer);

            AbstractOrchestratorFunctionHandlerRenderer orchestratorRenderer =
                FunctionHandlerRendererFactory.createOrchestratorRenderer(null);
            assertNotNull(orchestratorRenderer);
        });
    }

    @Test
    void createRendererSupportsQuarkusProfile() {
        AbstractFunctionHandlerRenderer renderer = FunctionHandlerRendererFactory.createRenderer("quarkus");
        AbstractOrchestratorFunctionHandlerRenderer orchestratorRenderer =
            FunctionHandlerRendererFactory.createOrchestratorRenderer("quarkus");

        assertNotNull(renderer);
        assertNotNull(orchestratorRenderer);
    }

    @Test
    void createRendererSupportsSpringProfile() {
        AbstractFunctionHandlerRenderer renderer = FunctionHandlerRendererFactory.createRenderer("spring");
        AbstractOrchestratorFunctionHandlerRenderer orchestratorRenderer =
            FunctionHandlerRendererFactory.createOrchestratorRenderer("spring");

        assertNotNull(renderer);
        assertNotNull(orchestratorRenderer);
    }

    @Test
    void createRendererRejectsUnsupportedProfile() {
        assertThrows(IllegalArgumentException.class, () -> FunctionHandlerRendererFactory.createRenderer("reactive"));
        assertThrows(IllegalArgumentException.class,
            () -> FunctionHandlerRendererFactory.createOrchestratorRenderer("reactive"));
    }
}
