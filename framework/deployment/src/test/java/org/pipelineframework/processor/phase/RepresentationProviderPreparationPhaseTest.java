package org.pipelineframework.processor.phase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.squareup.javapoet.ClassName;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.List;
import java.util.Set;

import com.squareup.javapoet.TypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.ServiceApiKind;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;
import org.pipelineframework.processor.representation.ResolvedProviderBoundary;
import org.pipelineframework.config.template.PipelineTemplateConfigLoader;
import org.pipelineframework.config.template.RepresentationMapping;
import org.pipelineframework.representation.spi.BoundaryClaim;
import org.pipelineframework.representation.spi.BoundaryRequest;
import org.pipelineframework.representation.spi.CanonicalType;
import org.pipelineframework.representation.spi.CanonicalTypeShape;
import org.pipelineframework.representation.spi.ProviderExecutionStyle;
import org.pipelineframework.representation.spi.ProviderStepContract;
import org.pipelineframework.representation.spi.RepresentationScope;

class RepresentationProviderPreparationPhaseTest {

    @TempDir Path tempDir;

    @Test
    void claimedBoundaryValidatesItsDeclaredTypeMappingWithoutGlobalConfiguration() {
        RepresentationMapping mapping = new RepresentationMapping("opencsv", "PaymentRecord",
            Optional.of("example.PaymentRow"), Optional.of("example.PaymentMapper"), Map.of("separator", ","));

        var configuration = RepresentationProviderPreparationPhase.typeConfiguration(
            new BoundaryClaim("opencsv", "Read Payments", "example.PaymentReader", Optional.empty()), mapping);

        assertEquals(RepresentationScope.TYPE, configuration.scope());
        assertEquals("opencsv", configuration.providerKey());
        assertEquals(Map.of("separator", ","), configuration.options());
    }

    @Test
    void resolvesCanonicalOwnerFromDeclaredJavaRepresentation() throws Exception {
        var config = load("""
            version: 3
            appName: File representation
            basePackage: org.pipelineframework
            types:
              Document:
                fields: [[sourceId, string], [content, payload_ref]]
                mappings:
                  file: { type: example.MaterializedDocument }
            """);

        var canonical = RepresentationProviderPreparationPhase.canonical(
            config, ClassName.get("example", "MaterializedDocument"));

        assertEquals("Document", canonical.name());
        assertEquals("org.pipelineframework.domain.Document", canonical.targetTypeName());
        assertEquals(org.pipelineframework.representation.spi.CanonicalTypeShape.RECORD, canonical.shape());
    }

    @Test
    void rejectsAmbiguousCanonicalOwnersForOneJavaRepresentation() throws Exception {
        var config = load("""
            version: 3
            appName: Ambiguous representation
            basePackage: org.pipelineframework
            types:
              First:
                fields: [[content, payload_ref]]
                mappings:
                  file: { type: example.MaterializedDocument }
              Second:
                fields: [[content, payload_ref]]
                mappings:
                  file: { type: example.MaterializedDocument }
            """);

        assertThrows(IllegalStateException.class, () -> RepresentationProviderPreparationPhase.canonical(
            config, ClassName.get("example", "MaterializedDocument")));
    }

    @Test
    void generatedReactiveFacadeReplacesBlockingAuthoredServiceContract() {
        PipelineStepModel authored = new PipelineStepModel.Builder()
            .serviceName("PrepareInvoice")
            .generatedName("PrepareInvoice")
            .servicePackage("example")
            .serviceClassName(ClassName.get("example", "PrepareInvoiceStep"))
            .inputMapping(new TypeMapping(TypeName.INT, TypeName.INT, false))
            .outputMapping(new TypeMapping(TypeName.INT, TypeName.INT, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(Set.of(GenerationTarget.CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .serviceApiKind(ServiceApiKind.BLOCKING)
            .build();
        CanonicalType input = new CanonicalType("Input", "example.Input", CanonicalTypeShape.RECORD);
        CanonicalType output = new CanonicalType("Output", "example.Output", CanonicalTypeShape.RECORD);
        BoundaryRequest request = new BoundaryRequest(
            "Prepare Invoice", "example.PrepareInvoiceStep", input, output, "UNARY_UNARY", Set.of(), Map.of());
        BoundaryClaim claim = new BoundaryClaim(
            "file", "Prepare Invoice:file", "example.PrepareInvoiceStepPipelineFacade",
            Optional.of(new ProviderStepContract(ProviderExecutionStyle.REACTIVE, "UNARY_UNARY")));
        PipelineCompilationContext context = new PipelineCompilationContext(null, null);
        context.setStepModels(List.of(authored));

        RepresentationProviderGenerationPhase.replaceServiceWithFacade(context,
            new ResolvedProviderBoundary(request, claim, List.of(), Map.of()));

        PipelineStepModel replaced = context.getStepModels().getFirst();
        assertEquals(ClassName.get("example", "PrepareInvoiceStepPipelineFacade"), replaced.serviceClassName());
        assertEquals(ServiceApiKind.REACTIVE, replaced.serviceApiKind());
    }

    private org.pipelineframework.config.template.PipelineTemplateConfig load(String yaml) throws Exception {
        Path path = tempDir.resolve("pipeline.yaml");
        Files.writeString(path, yaml);
        return new PipelineTemplateConfigLoader().load(path);
    }
}
