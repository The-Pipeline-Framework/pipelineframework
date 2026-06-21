package org.pipelineframework.processor.renderer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.PipelineTransport;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;

class QueryClientStepRendererTest {

    @TempDir
    Path tempDir;

    @Test
    void targetReturnsQueryClientStepTarget() {
        assertEquals(GenerationTarget.QUERY_CLIENT_STEP, new QueryClientStepRenderer().target());
    }

    @ParameterizedTest
    @ValueSource(strings = {"LOCAL", "REST", "GRPC"})
    void rendersReactiveStepThatDelegatesToQuerySupport(String transport) throws IOException {
        PipelineStepModel model = model(
            ClassName.get("com.example.common.domain", "CustomerRiskLookup"),
            ClassName.get("com.example.common.domain", "CustomerRiskSnapshot"));

        new QueryClientStepRenderer().render(model, generationContext(transport));

        String source = Files.readString(tempDir.resolve(
            "com/example/risk/pipeline/LoadCustomerRiskQueryClientStep.java"));

        switch (transport) {
            case "LOCAL" -> {
                assertTrue(source.contains("implements StepOneToOne<CustomerRiskLookup, CustomerRiskSnapshot>"));
                assertTrue(source.contains("\"com.example.common.domain.CustomerRiskLookup\", "
                    + "\"com.example.common.domain.CustomerRiskSnapshot\""));
            }
            case "REST" -> {
                assertTrue(source.contains("implements StepOneToOne<CustomerRiskLookupDto, CustomerRiskSnapshotDto>"));
                assertTrue(source.contains("\"com.example.common.dto.CustomerRiskLookupDto\", "
                    + "\"com.example.common.dto.CustomerRiskSnapshotDto\""));
            }
            case "GRPC" -> {
                assertTrue(source.contains(
                    "implements StepOneToOne<PipelineTypes.CustomerRiskLookup, PipelineTypes.CustomerRiskSnapshot>"));
                assertTrue(source.contains("\"com.example.grpc.PipelineTypes.CustomerRiskLookup\", "
                    + "\"com.example.grpc.PipelineTypes.CustomerRiskSnapshot\""));
            }
            default -> throw new IllegalArgumentException("Unexpected transport " + transport);
        }
        assertTrue(source.contains("QueryStepSupport support"));
        assertTrue(source.contains("QueryStepDescriptorFactory descriptorFactory"));
        assertTrue(source.contains("support.queryOneToOne(descriptorFactory.descriptor(\"LoadCustomerRisk\", "));
    }

    @Test
    void fallsBackToConfiguredBasePackageForNonStandardDomainPackage() throws IOException {
        PipelineStepModel model = model(
            ClassName.get("com.example.risk.domain", "CustomerRiskLookup"),
            ClassName.get("com.example.risk.domain", "CustomerRiskSnapshot"));

        new QueryClientStepRenderer().render(model, generationContext(PipelineTransport.REST, "com.example"));

        String source = Files.readString(tempDir.resolve(
            "com/example/risk/pipeline/LoadCustomerRiskQueryClientStep.java"));

        assertTrue(source.contains("import com.example.common.dto.CustomerRiskLookupDto;"));
        assertTrue(source.contains("import com.example.common.dto.CustomerRiskSnapshotDto;"));
    }

    @Test
    void usesConfiguredBasePackageForBlankDomainPackage() throws IOException {
        PipelineStepModel model = model(
            ClassName.get("", "CustomerRiskLookup"),
            ClassName.get("", "CustomerRiskSnapshot"));

        new QueryClientStepRenderer().render(model, generationContext(PipelineTransport.GRPC, "com.example"));

        String source = Files.readString(tempDir.resolve(
            "com/example/risk/pipeline/LoadCustomerRiskQueryClientStep.java"));

        assertTrue(source.contains("import com.example.grpc.PipelineTypes;"));
        assertTrue(source.contains(
            "implements StepOneToOne<PipelineTypes.CustomerRiskLookup, PipelineTypes.CustomerRiskSnapshot>"));
    }

    @Test
    void rejectsUnrecognizedDomainPackageWithoutConfiguredBasePackage() {
        PipelineStepModel model = model(
            ClassName.get("com.example.risk.domain", "CustomerRiskLookup"),
            ClassName.get("com.example.risk.domain", "CustomerRiskSnapshot"));

        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
            new QueryClientStepRenderer().render(model, generationContext(PipelineTransport.REST, null)));

        assertTrue(exception.getMessage().contains("does not match .common.domain, .common.dto, or .service"));
    }

    private PipelineStepModel model(ClassName inputType, ClassName outputType) {
        return new PipelineStepModel.Builder()
            .serviceName("LoadCustomerRisk")
            .generatedName("LoadCustomerRiskService")
            .servicePackage("com.example.risk")
            .serviceClassName(ClassName.get("org.pipelineframework.query", "QueryStepDescriptor"))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.DEFAULT)
            .inputMapping(new TypeMapping(inputType, null, false))
            .outputMapping(new TypeMapping(outputType, null, false))
            .enabledTargets(Set.of(GenerationTarget.QUERY_CLIENT_STEP))
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .build();
    }

    private GenerationContext generationContext(String transport) {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(Map.of("pipeline.transport", transport));
        return new GenerationContext(
            processingEnv,
            tempDir,
            DeploymentRole.ORCHESTRATOR_CLIENT,
            Set.of(),
            null,
            null);
    }

    private GenerationContext generationContext(PipelineTransport transport, String basePackage) {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(Map.of());
        return new GenerationContext(
            processingEnv,
            tempDir,
            DeploymentRole.ORCHESTRATOR_CLIENT,
            Set.of(),
            null,
            null,
            transport,
            basePackage);
    }
}
