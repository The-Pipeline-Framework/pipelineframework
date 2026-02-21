package org.pipelineframework.processor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.config.PipelineStepConfigLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineStepConfigLoaderTest {

    @Test
    void loadsOutputTypesFromPipelineConfig(@TempDir Path tempDir) throws IOException {
        PipelineStepConfigLoader loader = new PipelineStepConfigLoader();
        Path config = tempDir.resolve("pipeline-config.yaml");
        Files.writeString(config, """
            appName: "test"
            basePackage: "org.pipelineframework.csv"
            steps:
              - name: "Process Folder"
                cardinality: "EXPANSION"
                inputTypeName: "CsvFolder"
                outputTypeName: "CsvPaymentsInputFile"
              - name: "Process Csv Payments Input"
                cardinality: "EXPANSION"
                inputTypeName: "CsvPaymentsInputFile"
                outputTypeName: "PaymentRecord"
              - name: "Process Send Payment Record"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "PaymentRecord"
                outputTypeName: "PaymentStatus"
            """);
        PipelineStepConfigLoader.StepConfig stepConfig = loader.load(config);

        assertEquals("org.pipelineframework.csv", stepConfig.basePackage());
        assertTrue(stepConfig.inputTypes().contains("CsvFolder"));
        assertTrue(stepConfig.inputTypes().contains("CsvPaymentsInputFile"));
        assertTrue(stepConfig.inputTypes().contains("PaymentRecord"));
        assertTrue(stepConfig.outputTypes().contains("CsvPaymentsInputFile"));
        assertTrue(stepConfig.outputTypes().contains("PaymentRecord"));
        assertTrue(stepConfig.outputTypes().contains("PaymentStatus"));
        assertEquals("COMPUTE", stepConfig.platform());
    }

    @Test
    void transportOverridePrefersSystemPropertyOverEnv(@TempDir Path tempDir) throws IOException {
        PipelineStepConfigLoader loader = new PipelineStepConfigLoader(
            key -> "pipeline.transport".equals(key) ? "LOCAL" : null,
            key -> "PIPELINE_TRANSPORT".equals(key) ? "REST" : null
        );
        Path config = tempDir.resolve("pipeline-config.yaml");
        Files.writeString(config, "basePackage: test\ntransport: GRPC\nsteps: []\n");

        PipelineStepConfigLoader.StepConfig stepConfig = loader.load(config);

        assertEquals("LOCAL", stepConfig.transport());
        assertEquals("COMPUTE", stepConfig.platform());
    }

    @Test
    void transportOverrideFallsBackToEnvWhenPropertyMissing(@TempDir Path tempDir) throws IOException {
        PipelineStepConfigLoader loader = new PipelineStepConfigLoader(
            key -> null,
            key -> "PIPELINE_TRANSPORT".equals(key) ? "REST" : null
        );
        Path config = tempDir.resolve("pipeline-config.yaml");
        Files.writeString(config, "basePackage: test\ntransport: GRPC\nsteps: []\n");

        PipelineStepConfigLoader.StepConfig stepConfig = loader.load(config);

        assertEquals("REST", stepConfig.transport());
        assertEquals("COMPUTE", stepConfig.platform());
    }

    @Test
    void platformOverridePrefersSystemPropertyOverEnv(@TempDir Path tempDir) throws IOException {
        PipelineStepConfigLoader loader = new PipelineStepConfigLoader(
            key -> "pipeline.platform".equals(key) ? "LAMBDA" : null,
            key -> "PIPELINE_PLATFORM".equals(key) ? "COMPUTE" : null
        );
        Path config = tempDir.resolve("pipeline-config.yaml");
        Files.writeString(config, "basePackage: test\nplatform: COMPUTE\nsteps: []\n");

        PipelineStepConfigLoader.StepConfig stepConfig = loader.load(config);

        assertEquals("FUNCTION", stepConfig.platform());
    }

    @Test
    void platformOverrideFallsBackToEnvWhenPropertyMissing(@TempDir Path tempDir) throws IOException {
        PipelineStepConfigLoader loader = new PipelineStepConfigLoader(
            key -> null,
            key -> "PIPELINE_PLATFORM".equals(key) ? "LAMBDA" : null
        );
        Path config = tempDir.resolve("pipeline-config.yaml");
        Files.writeString(config, "basePackage: test\nplatform: COMPUTE\nsteps: []\n");

        PipelineStepConfigLoader.StepConfig stepConfig = loader.load(config);

        assertEquals("FUNCTION", stepConfig.platform());
    }

    @Test
    void transportPrecedenceProcessorOptionOverridesYamlAndEnv(@TempDir Path tempDir) throws IOException {
        // Precedence: processor option > env var > YAML config > GRPC default
        PipelineStepConfigLoader loader = new PipelineStepConfigLoader(
            key -> "pipeline.transport".equals(key) ? "LOCAL" : null,
            key -> "PIPELINE_TRANSPORT".equals(key) ? "REST" : null
        );
        Path config = tempDir.resolve("pipeline-config.yaml");
        Files.writeString(config, "basePackage: test\ntransport: GRPC\nsteps: []\n");

        PipelineStepConfigLoader.StepConfig stepConfig = loader.load(config);

        assertEquals("LOCAL", stepConfig.transport(), "Processor option should override YAML and env");
    }

    @Test
    void transportPrecedenceEnvOverridesYaml(@TempDir Path tempDir) throws IOException {
        // When processor option is absent, env var should override YAML
        PipelineStepConfigLoader loader = new PipelineStepConfigLoader(
            key -> null,
            key -> "PIPELINE_TRANSPORT".equals(key) ? "LOCAL" : null
        );
        Path config = tempDir.resolve("pipeline-config.yaml");
        Files.writeString(config, "basePackage: test\ntransport: GRPC\nsteps: []\n");

        PipelineStepConfigLoader.StepConfig stepConfig = loader.load(config);

        assertEquals("LOCAL", stepConfig.transport(), "Env var should override YAML config");
    }

    @Test
    void transportPrecedenceYamlUsedWhenNoOverride(@TempDir Path tempDir) throws IOException {
        // When no overrides, YAML config should be used
        PipelineStepConfigLoader loader = new PipelineStepConfigLoader(
            key -> null,
            key -> null
        );
        Path config = tempDir.resolve("pipeline-config.yaml");
        Files.writeString(config, "basePackage: test\ntransport: REST\nsteps: []\n");

        PipelineStepConfigLoader.StepConfig stepConfig = loader.load(config);

        assertEquals("REST", stepConfig.transport(), "YAML config should be used when no overrides");
    }

    @Test
    void transportPrecedenceGrpcDefaultWhenNoYamlOrOverride(@TempDir Path tempDir) throws IOException {
        // When no transport in YAML and no overrides, should default to GRPC
        PipelineStepConfigLoader loader = new PipelineStepConfigLoader(
            key -> null,
            key -> null
        );
        Path config = tempDir.resolve("pipeline-config.yaml");
        Files.writeString(config, "basePackage: test\nsteps: []\n");

        PipelineStepConfigLoader.StepConfig stepConfig = loader.load(config);

        assertEquals("GRPC", stepConfig.transport(), "Should default to GRPC when no transport specified");
    }
}
