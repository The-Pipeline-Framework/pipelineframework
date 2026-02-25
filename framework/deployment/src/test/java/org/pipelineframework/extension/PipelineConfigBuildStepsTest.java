package org.pipelineframework.extension;

import jakarta.enterprise.inject.spi.DeploymentException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Execution(ExecutionMode.SAME_THREAD)
class PipelineConfigBuildStepsTest {

    @TempDir
    Path tempDir;
    private String prevConfig;
    private String prevUserDir;

    @BeforeEach
    void saveProperties() {
        prevConfig = System.getProperty("pipeline.config");
        prevUserDir = System.getProperty("user.dir");
    }

    @AfterEach
    void restoreProperties() {
        restoreProperty("pipeline.config", prevConfig);
        restoreProperty("user.dir", prevUserDir);
    }

    @Test
    void loadsDelegatedStepsFromExplicitPipelineConfig() throws Exception {
        Path config = tempDir.resolve("pipeline.yaml");
        Files.writeString(config, """
            steps:
              - name: "Delegated A"
                operator: "com.acme.operators.Foo::run"
              - name: "Internal B"
                service: "com.acme.service.InternalStep"
            """);

        System.setProperty("pipeline.config", config.toString());
        PipelineConfigBuildItem item = new PipelineConfigBuildSteps().loadPipelineConfig();

        assertEquals(1, item.steps().size());
        PipelineConfigBuildItem.StepConfig step = item.steps().get(0);
        assertEquals("Delegated A", step.name());
        assertEquals("com.acme.operators.Foo::run", step.operator());
    }

    @Test
    void failsWhenOperatorAndDelegateConflict() throws Exception {
        Path config = tempDir.resolve("pipeline.yaml");
        Files.writeString(config, """
            steps:
              - name: "Delegated A"
                operator: "com.acme.operators.Foo::run"
                delegate: "com.acme.operators.Bar::run"
            """);

        System.setProperty("pipeline.config", config.toString());
        DeploymentException ex = assertThrows(
                DeploymentException.class,
                () -> new PipelineConfigBuildSteps().loadPipelineConfig());
        assertTrue(ex.getMessage().contains("defines both operator and delegate"));
    }

    private static void restoreProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }
}
