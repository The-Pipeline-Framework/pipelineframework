/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.processor.composition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.CardinalitySemantics;
import org.pipelineframework.orchestrator.composition.PipelineCompositionContinuationKind;
import org.pipelineframework.orchestrator.composition.PipelineCompositionDescriptor;

class PipelineCompositionContractProjectorTest {

    @Test
    void canonicalizesDefinitionOrderIndependentlyOfResolvedMapInsertionOrder() {
        PipelineReference root = new PipelineReference("z-root");
        PipelineReference child = new PipelineReference("a-child");
        PipelineDefinition childDefinition = definition(child, "Value", "Value",
            PipelineDefinitionStep.direct("x", "Value", "Value", CardinalitySemantics.ONE_TO_ONE));
        PipelineDefinition rootDefinition = definition(root, "Value", "Value",
            PipelineDefinitionStep.pipeline("invoke", "Value", "Value", child));
        ResolvedPipelineDefinitionGraph linked = linker(Map.of(child, childDefinition)).link(rootDefinition);

        Map<PipelineReference, PipelineDefinition> childFirst = new LinkedHashMap<>();
        childFirst.put(child, childDefinition);
        childFirst.put(root, rootDefinition);
        ResolvedPipelineDefinitionGraph reordered = new ResolvedPipelineDefinitionGraph(
            linked.root(),
            childFirst,
            linked.rootCardinality(),
            linked.invocationBindings(),
            linked.continuationRoutes());

        PipelineCompositionContractProjector projector = new PipelineCompositionContractProjector();
        PipelineCompositionDescriptor first = projector.project(linked);
        PipelineCompositionDescriptor second = projector.project(reordered);

        assertEquals(first.definitions(), second.definitions());
        assertEquals(List.of("a-child", "z-root"),
            first.definitions().stream().map(definition -> definition.definitionId()).toList());
    }

    @Test
    void projectsStructuredReturnRoutesRatherThanFlattenedExecutableLocations() {
        PipelineReference outer = new PipelineReference("outer");
        PipelineReference middle = new PipelineReference("middle");
        PipelineReference inner = new PipelineReference("inner");
        PipelineDefinition innerDefinition = definition(inner, "Value", "Value",
            PipelineDefinitionStep.direct("x", "Value", "Value", CardinalitySemantics.ONE_TO_ONE),
            PipelineDefinitionStep.direct("await", "Value", "Value", CardinalitySemantics.ONE_TO_ONE),
            PipelineDefinitionStep.direct("y", "Value", "Value", CardinalitySemantics.ONE_TO_ONE));
        PipelineDefinition middleDefinition = definition(middle, "Value", "Value",
            PipelineDefinitionStep.pipeline("call-inner", "Value", "Value", inner),
            PipelineDefinitionStep.direct("after-inner", "Value", "Value", CardinalitySemantics.ONE_TO_ONE));
        PipelineDefinition outerDefinition = definition(outer, "Value", "Value",
            PipelineDefinitionStep.direct("a", "Value", "Value", CardinalitySemantics.ONE_TO_ONE),
            PipelineDefinitionStep.pipeline("call-middle", "Value", "Value", middle),
            PipelineDefinitionStep.direct("c", "Value", "Value", CardinalitySemantics.ONE_TO_ONE));

        PipelineCompositionDescriptor composition = new PipelineCompositionContractProjector().project(
            linker(Map.of(middle, middleDefinition, inner, innerDefinition)).link(outerDefinition));

        assertEquals("outer", composition.rootDefinitionId());
        assertEquals(PipelineCompositionContinuationKind.NEXT_LOCAL,
            composition.definition("outer").continuation("call-middle").kind());
        assertEquals(PipelineCompositionContinuationKind.RETURN,
            composition.definition("inner").continuation("y").kind());
        assertEquals(PipelineCompositionContinuationKind.RETURN,
            composition.definition("middle").continuation("after-inner").kind());
        assertEquals(PipelineCompositionContinuationKind.ROOT_TERMINAL,
            composition.definition("outer").continuation("c").kind());
        assertEquals("middle", composition.definition("outer").node("call-middle").targetDefinitionId());
        assertEquals(CardinalitySemantics.ONE_TO_ONE.name(),
            composition.definition("outer").node("call-middle").cardinality());
    }

    @Test
    void pinsRoutingDeclarationsWithoutChangingStructuredContinuationResolution() {
        PipelineReference child = new PipelineReference("child");
        PipelineDefinition childDefinition = definition(child, "Request", "Final",
            PipelineDefinitionStep.direct("classify", "Request", "Decision", CardinalitySemantics.ONE_TO_ONE),
            PipelineDefinitionStep.direct("handle-physical", "Decision", "PhysicalResult", CardinalitySemantics.ONE_TO_ONE,
                List.of("Physical"), false),
            PipelineDefinitionStep.direct("finish", "Result", "Final", CardinalitySemantics.ONE_TO_ONE,
                List.of("PhysicalResult"), true));
        PipelineReference root = new PipelineReference("root");
        PipelineDefinition rootDefinition = definition(root, "Request", "Final",
            PipelineDefinitionStep.pipeline("invoke", "Request", "Final", child));

        PipelineCompositionDescriptor first = new PipelineCompositionContractProjector().project(
            linker(Map.of(child, childDefinition)).link(rootDefinition));
        PipelineCompositionDescriptor second = new PipelineCompositionContractProjector().project(
            linker(Map.of(child, childDefinition)).link(rootDefinition));

        assertEquals(first.definition("child").definitionFingerprint(), second.definition("child").definitionFingerprint());
        assertEquals(List.of("Physical"), first.definition("child").node("handle-physical").acceptedContractIds());
        assertTrue(first.definition("child").node("finish").terminal());
        assertEquals(PipelineCompositionContinuationKind.NEXT_LOCAL,
            first.definition("child").continuation("handle-physical").kind());
        assertEquals(PipelineCompositionContinuationKind.RETURN,
            first.definition("child").continuation("finish").kind());
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
}
