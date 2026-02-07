package org.pipelineframework.processor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.pipelineframework.processor.config.PipelineStepConfigLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineStepConfigLoaderTest {

    @Test
    void loadsOutputTypesFromPipelineConfig() throws IOException {
        PipelineStepConfigLoader loader = new PipelineStepConfigLoader();
        Path config = Files.createTempFile("pipeline-config", ".yaml");
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
    }

    @Test
    void transportOverridePrefersSystemPropertyOverEnv() throws IOException {
        PipelineStepConfigLoader loader = new PipelineStepConfigLoader(
            key -> "pipeline.transport".equals(key) ? "LOCAL" : null,
            key -> "PIPELINE_TRANSPORT".equals(key) ? "REST" : null
        );
        Path config = Files.createTempFile("pipeline-config", ".yaml");
        Files.writeString(config, "basePackage: test\ntransport: GRPC\nsteps: []\n");

        PipelineStepConfigLoader.StepConfig stepConfig = loader.load(config);

        assertEquals("LOCAL", stepConfig.transport());
    }

    @Test
    void transportOverrideFallsBackToEnvWhenPropertyMissing() throws IOException {
        PipelineStepConfigLoader loader = new PipelineStepConfigLoader(
            key -> null,
            key -> "PIPELINE_TRANSPORT".equals(key) ? "REST" : null
        );
        Path config = Files.createTempFile("pipeline-config", ".yaml");
        Files.writeString(config, "basePackage: test\ntransport: GRPC\nsteps: []\n");

        PipelineStepConfigLoader.StepConfig stepConfig = loader.load(config);

        assertEquals("REST", stepConfig.transport());
    }
}
