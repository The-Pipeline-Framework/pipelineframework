package org.pipelineframework.processor.phase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.squareup.javapoet.ClassName;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.pipelineframework.processor.composition.PipelineReference;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.representation.ResolvedProviderBoundary;
import org.pipelineframework.representation.spi.BoundaryClaim;
import org.pipelineframework.representation.spi.BoundaryRequest;
import org.pipelineframework.representation.spi.CanonicalType;
import org.pipelineframework.representation.spi.CanonicalTypeShape;
import org.pipelineframework.representation.spi.ProviderExecutionStyle;
import org.pipelineframework.representation.spi.ProviderStepContract;

class RepresentationProviderGenerationPhaseTest {

    @Test
    void matchesTheExactBoundaryStepWhenDefinitionsReuseAServiceClass() {
        ResolvedProviderBoundary boundary = boundary("Extract document");

        assertTrue(RepresentationProviderGenerationPhase.isBoundaryModel(
            model("ProcessExtractDocumentService", "org.example/document-block"), boundary));
        assertFalse(RepresentationProviderGenerationPhase.isBoundaryModel(
            model("ProcessInspectDocumentService", "org.example/document-block"), boundary));
    }

    @Test
    void nestedBoundaryDoesNotReplaceAnIdenticalRootModel() {
        ResolvedProviderBoundary boundary = boundary("Extract document", "org.example/document-block");
        PipelineStepModel root = model("ProcessExtractDocumentService", "$root");
        PipelineStepModel nested = model("ProcessExtractDocumentService", "org.example/document-block");
        var context = new org.pipelineframework.processor.PipelineCompilationContext(null, null);
        context.setStepModels(List.of(root));
        context.setLocalDefinitionStepModels(Map.of("org.example/document-block", List.of(nested)));

        RepresentationProviderGenerationPhase.replaceServiceWithFacade(context, boundary);

        assertEquals(root.serviceClassName(), context.getStepModels().getFirst().serviceClassName());
        assertEquals(ClassName.bestGuess("example.GeneratedDocumentFacade"),
            context.getLocalDefinitionStepModels().get("org.example/document-block")
                .getFirst().serviceClassName());
    }

    private static ResolvedProviderBoundary boundary(String stepName) {
        return boundary(stepName, "org.example/document-block");
    }

    private static ResolvedProviderBoundary boundary(String stepName, String definition) {
        CanonicalType type = new CanonicalType("Document", "example.Document", CanonicalTypeShape.RECORD);
        return new ResolvedProviderBoundary(
            new PipelineReference(definition),
            new BoundaryRequest(stepName, "example.SharedDocumentService", type, type, "ONE_TO_ONE",
                Set.of(), Map.of()),
            new BoundaryClaim("file", "extract:file", "example.GeneratedDocumentFacade",
                Optional.of(new ProviderStepContract(ProviderExecutionStyle.REACTIVE, "UNARY_UNARY"))),
            List.of(),
            Map.of());
    }

    private static PipelineStepModel model(String serviceName, String definition) {
        return new PipelineStepModel.Builder()
            .definition(new PipelineReference(definition))
            .serviceName(serviceName)
            .generatedName(serviceName)
            .servicePackage("example")
            .serviceClassName(ClassName.get("example", "SharedDocumentService"))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .enabledTargets(Set.of(GenerationTarget.LOCAL_CLIENT_STEP))
            .build();
    }
}
