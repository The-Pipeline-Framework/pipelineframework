package org.pipelineframework.processor.mapping;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.config.PipelineRuntimeMappingLoader;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class PipelineRuntimeMappingResolverTest {

    @TempDir
    Path tempDir;

    @Test
    void resolvesExplicitStepAndSyntheticMappings() throws IOException {
        Path mappingFile = tempDir.resolve("pipeline.runtime.yaml");
        Files.writeString(mappingFile, """
            version: 1
            layout: modular
            validation: auto
            runtimes:
              local: {}
            modules:
              payments:
                runtime: local
              cache:
                runtime: local
            steps:
              ValidateCard:
                module: payments
            synthetics:
              ObserveLatency.Payload:
                module: cache
            """);

        PipelineRuntimeMapping mapping = new PipelineRuntimeMappingLoader().load(mappingFile);
        PipelineStepModel step = stepModel("ProcessValidateCardService", "org.example.payments.service");
        PipelineStepModel synthetic = syntheticModel(
            "ObserveLatencyPayloadSideEffectService",
            "org.example.payments.service",
            "Payload");

        assertNotNull(step);
        assertNotNull(synthetic);

        PipelineRuntimeMappingResolver resolver = new PipelineRuntimeMappingResolver(mapping, null);
        PipelineRuntimeMappingResolution resolution = resolver.resolve(List.of(step, synthetic));

        assertEquals("payments", resolution.moduleAssignments().get(step));
        assertEquals("cache", resolution.moduleAssignments().get(synthetic));
        assertEquals("payments", resolution.clientOverrides().get("process-validate-card"));
        assertEquals("cache", resolution.clientOverrides().get("observe-latency-payload-side-effect"));
    }

    private PipelineStepModel stepModel(String serviceName, String servicePackage) {
        ClassName className = ClassName.get(servicePackage, serviceName);
        return new PipelineStepModel.Builder()
            .serviceName(serviceName)
            .servicePackage(servicePackage)
            .serviceClassName(className)
            .inputMapping(new TypeMapping(ClassName.get("org.example", "Input"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("org.example", "Output"), null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(Set.of())
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .build();
    }

    private PipelineStepModel syntheticModel(String serviceName, String servicePackage, String payloadType) {
        ClassName className = ClassName.get(servicePackage, serviceName);
        TypeMapping mapping = new TypeMapping(ClassName.get("org.example", payloadType), null, false);
        return new PipelineStepModel.Builder()
            .serviceName(serviceName)
            .servicePackage(servicePackage)
            .serviceClassName(className)
            .inputMapping(mapping)
            .outputMapping(mapping)
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(Set.of())
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PLUGIN_SERVER)
            .sideEffect(true)
            .build();
    }
}
