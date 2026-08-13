package org.pipelineframework.processor.composition;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.CardinalitySemantics;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeModel;
import org.pipelineframework.config.template.PipelineTemplateTypeReference;
import org.pipelineframework.processor.routing.V3PipelineInvocationCompatibility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineDefinitionLinkerTest {

    @Test
    void derivesReferencedPipelineCardinalityWithoutTerminalShapeInference() {
        PipelineReference innerReference = new PipelineReference("fraud-analysis");
        PipelineDefinition inner = definition(
            innerReference,
            "Request",
            "Assessment",
            PipelineDefinitionStep.direct(
                "aggregate-signals",
                "Request",
                "SignalSummary",
                CardinalitySemantics.MANY_TO_ONE),
            PipelineDefinitionStep.direct(
                "expand-assessment",
                "SignalSummary",
                "Assessment",
                CardinalitySemantics.ONE_TO_MANY));
        PipelineDefinition root = definition(
            new PipelineReference("incident-intake"),
            "Request",
            "Assessment",
            PipelineDefinitionStep.pipeline("analyse", "Request", "Assessment", innerReference));

        ResolvedPipelineDefinitionGraph graph = linker(Map.of(innerReference, inner)).link(root);

        assertEquals(CardinalitySemantics.MANY_TO_MANY, graph.rootCardinality());
        assertEquals(CardinalitySemantics.MANY_TO_MANY, graph.invocationBindings().getFirst().cardinality());
    }

    @Test
    void expandsDuplicateReferencesIntoDistinctCompiledLocationsAndRoutesInnerSuffix() {
        PipelineReference innerReference = new PipelineReference("tpf.agent");
        PipelineDefinition inner = definition(
            innerReference,
            "Turn",
            "Turn",
            PipelineDefinitionStep.direct("infer", "Turn", "Turn", CardinalitySemantics.ONE_TO_ONE),
            PipelineDefinitionStep.direct("await-authority", "Turn", "Turn", CardinalitySemantics.ONE_TO_ONE),
            PipelineDefinitionStep.direct("observe", "Turn", "Turn", CardinalitySemantics.ONE_TO_ONE));
        PipelineReference rootReference = new PipelineReference("outer");
        PipelineDefinition root = definition(
            rootReference,
            "Turn",
            "Turn",
            PipelineDefinitionStep.direct("prepare", "Turn", "Turn", CardinalitySemantics.ONE_TO_ONE),
            PipelineDefinitionStep.pipeline("first-turn", "Turn", "Turn", innerReference),
            PipelineDefinitionStep.direct("between-turns", "Turn", "Turn", CardinalitySemantics.ONE_TO_ONE),
            PipelineDefinitionStep.pipeline("second-turn", "Turn", "Turn", innerReference),
            PipelineDefinitionStep.direct("finish", "Turn", "Turn", CardinalitySemantics.ONE_TO_ONE));

        ResolvedPipelineDefinitionGraph graph = linker(Map.of(innerReference, inner)).link(root);

        CompiledPipelineLocation firstAwait = location(rootReference, "first-turn", innerReference, "await-authority");
        CompiledPipelineLocation secondAwait = location(rootReference, "second-turn", innerReference, "await-authority");
        CompiledPipelineLocation firstObserve = location(rootReference, "first-turn", innerReference, "observe");
        CompiledPipelineLocation betweenTurns = location(rootReference, "between-turns");

        assertNotEquals(firstAwait, secondAwait);
        assertEquals("outer:first-turn/tpf.agent:await-authority", firstAwait.display());
        assertEquals(firstObserve, graph.continuationAfter(firstAwait).orElseThrow());
        assertEquals(betweenTurns, graph.continuationAfter(firstObserve).orElseThrow());
        assertEquals(2, graph.invocationBindings().size());
        assertNotEquals(
            graph.invocationBindings().get(0).invocationLocation(),
            graph.invocationBindings().get(1).invocationLocation());
    }

    @Test
    void rejectsUnresolvedReferencesAndStaticRecursion() {
        PipelineReference missing = new PipelineReference("missing");
        PipelineDefinition unresolved = definition(
            new PipelineReference("root"),
            "Input",
            "Output",
            PipelineDefinitionStep.pipeline("call", "Input", "Output", missing));

        IllegalArgumentException unresolvedFailure = assertThrows(
            IllegalArgumentException.class,
            () -> linker(Map.of()).link(unresolved));
        assertEquals("Static pipeline reference could not be resolved: missing", unresolvedFailure.getMessage());

        PipelineReference recursiveReference = new PipelineReference("recursive");
        PipelineDefinition recursive = definition(
            recursiveReference,
            "Value",
            "Value",
            PipelineDefinitionStep.pipeline("again", "Value", "Value", recursiveReference));

        IllegalArgumentException recursionFailure = assertThrows(
            IllegalArgumentException.class,
            () -> linker(Map.of(recursiveReference, recursive)).link(recursive));
        assertEquals("Static pipeline definition cycle is not supported: recursive -> recursive",
            recursionFailure.getMessage());
    }

    @Test
    void rejectsInvocationContractMismatchAtLinkTime() {
        PipelineReference targetReference = new PipelineReference("target");
        PipelineDefinition target = definition(
            targetReference,
            "ExpectedInput",
            "ExpectedOutput",
            PipelineDefinitionStep.direct(
                "step",
                "ExpectedInput",
                "ExpectedOutput",
                CardinalitySemantics.ONE_TO_ONE));
        PipelineDefinition root = definition(
            new PipelineReference("root"),
            "ActualInput",
            "ActualOutput",
            PipelineDefinitionStep.pipeline("call", "ActualInput", "ActualOutput", targetReference));

        IllegalArgumentException failure = assertThrows(
            IllegalArgumentException.class,
            () -> linker(Map.of(targetReference, target)).link(root));

        assertEquals("Pipeline reference target input contract does not match callsite call", failure.getMessage());
    }

    @Test
    void acceptsV3UnionPayloadsAtAnInvocationCallsite() {
        PipelineTemplateTypeModel typeModel = new PipelineTemplateTypeModel(Map.of(
            "Decision", new PipelineTemplateTypeDefinition.UnionType("Decision", Map.of(
                "physical", new PipelineTemplateTypeDefinition.Variant(
                    "physical", new PipelineTemplateTypeReference.Named("PhysicalOrder")))),
            "PhysicalOrder", new PipelineTemplateTypeDefinition.RecordType("PhysicalOrder", List.of()),
            "Assessment", new PipelineTemplateTypeDefinition.RecordType("Assessment", List.of()),
            "AnalysisResult", new PipelineTemplateTypeDefinition.UnionType("AnalysisResult", Map.of(
                "assessment", new PipelineTemplateTypeDefinition.Variant(
                    "assessment", new PipelineTemplateTypeReference.Named("Assessment"))))));
        PipelineReference targetReference = new PipelineReference("physical-analysis");
        PipelineDefinition target = definition(
            targetReference,
            "PhysicalOrder",
            "Assessment",
            PipelineDefinitionStep.direct("analyse", "PhysicalOrder", "Assessment", CardinalitySemantics.ONE_TO_ONE));
        PipelineDefinition root = definition(
            new PipelineReference("root"),
            "Decision",
            "AnalysisResult",
            PipelineDefinitionStep.pipeline("analyse-physical", "Decision", "AnalysisResult", targetReference));

        ResolvedPipelineDefinitionGraph graph = new PipelineDefinitionLinker(
            reference -> java.util.Optional.ofNullable(Map.of(targetReference, target).get(reference)),
            new V3PipelineInvocationCompatibility(typeModel)).link(root);

        assertEquals(CardinalitySemantics.ONE_TO_ONE, graph.rootCardinality());
    }

    @Test
    void representsRoutedChildStepsWithoutRequiringImmediatePredecessorEquality() {
        PipelineReference childReference = new PipelineReference("decision-routing");
        PipelineDefinition child = definition(
            childReference,
            "OrderRequest",
            "FinalizedOrder",
            PipelineDefinitionStep.direct("classify", "OrderRequest", "OrderDecision", CardinalitySemantics.ONE_TO_ONE),
            PipelineDefinitionStep.direct("reserve", "OrderDecision", "StockReserved", CardinalitySemantics.ONE_TO_ONE,
                List.of("PhysicalOrder"), false),
            PipelineDefinitionStep.direct("provision", "OrderDecision", "LicenseProvisioned", CardinalitySemantics.ONE_TO_ONE,
                List.of("DigitalOrder"), false),
            PipelineDefinitionStep.direct("finalize", "OrderCompletion", "FinalizedOrder", CardinalitySemantics.ONE_TO_ONE,
                List.of("StockReserved", "LicenseProvisioned"), true));
        PipelineDefinition root = definition(
            new PipelineReference("root"),
            "OrderRequest",
            "FinalizedOrder",
            PipelineDefinitionStep.pipeline("route", "OrderRequest", "FinalizedOrder", childReference));

        ResolvedPipelineDefinitionGraph graph = linker(Map.of(childReference, child)).link(root);

        assertEquals(CardinalitySemantics.ONE_TO_ONE, graph.rootCardinality());
        assertEquals(List.of("PhysicalOrder"), child.steps().get(1).acceptedContractIds());
        assertEquals(List.of("DigitalOrder"), child.steps().get(2).acceptedContractIds());
        assertTrue(child.steps().get(3).terminal());
    }

    @Test
    void rejectsResolverDefinitionWithMismatchedIdentityAtLinkTime() {
        PipelineReference requested = new PipelineReference("requested");
        PipelineDefinition wrongDefinition = definition(
            new PipelineReference("wrong"),
            "Input",
            "Output",
            PipelineDefinitionStep.direct("step", "Input", "Output", CardinalitySemantics.ONE_TO_ONE));
        PipelineDefinition root = definition(
            new PipelineReference("root"),
            "Input",
            "Output",
            PipelineDefinitionStep.pipeline("call", "Input", "Output", requested));

        IllegalArgumentException failure = assertThrows(
            IllegalArgumentException.class,
            () -> new PipelineDefinitionLinker(reference -> java.util.Optional.of(wrongDefinition)).link(root));

        assertEquals(
            "Resolved definition reference does not match requested reference: requested",
            failure.getMessage());
    }

    private static PipelineDefinitionLinker linker(Map<PipelineReference, PipelineDefinition> definitions) {
        return new PipelineDefinitionLinker(reference -> java.util.Optional.ofNullable(definitions.get(reference)));
    }

    private static PipelineDefinition definition(
        PipelineReference reference,
        String input,
        String output,
        PipelineDefinitionStep... steps
    ) {
        return new PipelineDefinition(reference, input, output, List.of(steps));
    }

    private static CompiledPipelineLocation location(
        PipelineReference root,
        String rootCallsite,
        PipelineReference definition,
        String localStep
    ) {
        return new CompiledPipelineLocation(
            List.of(new DefinitionLocalLocation(root, rootCallsite)),
            new DefinitionLocalLocation(definition, localStep));
    }

    private static CompiledPipelineLocation location(PipelineReference definition, String localStep) {
        return new CompiledPipelineLocation(List.of(), new DefinitionLocalLocation(definition, localStep));
    }
}
