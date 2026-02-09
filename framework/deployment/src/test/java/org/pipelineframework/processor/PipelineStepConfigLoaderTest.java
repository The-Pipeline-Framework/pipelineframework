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
        assertEquals("STANDARD", stepConfig.platform());
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
        assertEquals("STANDARD", stepConfig.platform());
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
        assertEquals("STANDARD", stepConfig.platform());
    }

    @Test
    void platformOverridePrefersSystemPropertyOverEnv(@TempDir Path tempDir) throws IOException {
        PipelineStepConfigLoader loader = new PipelineStepConfigLoader(
            key -> "pipeline.platform".equals(key) ? "LAMBDA" : null,
            key -> "PIPELINE_PLATFORM".equals(key) ? "STANDARD" : null
        );
        Path config = tempDir.resolve("pipeline-config.yaml");
        Files.writeString(config, "basePackage: test\nplatform: STANDARD\nsteps: []\n");

        PipelineStepConfigLoader.StepConfig stepConfig = loader.load(config);

        assertEquals("LAMBDA", stepConfig.platform());
    }

    @Test
    void platformOverrideFallsBackToEnvWhenPropertyMissing(@TempDir Path tempDir) throws IOException {
        PipelineStepConfigLoader loader = new PipelineStepConfigLoader(
            key -> null,
            key -> "PIPELINE_PLATFORM".equals(key) ? "LAMBDA" : null
        );
        Path config = tempDir.resolve("pipeline-config.yaml");
        Files.writeString(config, "basePackage: test\nplatform: STANDARD\nsteps: []\n");

        PipelineStepConfigLoader.StepConfig stepConfig = loader.load(config);

        assertEquals("LAMBDA", stepConfig.platform());
    }
}
