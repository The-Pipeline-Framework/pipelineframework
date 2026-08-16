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

package org.pipelineframework.orchestrator.composition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

class PipelineCompositionResolverTest {

    @Test
    void resolvesNestedReturnThroughThePinnedStructuredContract() {
        PipelineCompositionResolver resolver = new PipelineCompositionResolver(composition());
        PipelineStaticLocation await = location(
            List.of(ref("outer", "call-middle"), ref("middle", "call-inner")), ref("inner", "await"));

        PipelineStaticLocation innerY = next(resolver.advance(await));
        assertEquals(location(List.of(ref("outer", "call-middle"), ref("middle", "call-inner")), ref("inner", "y")), innerY);
        assertEquals(1, resolver.rootStepIndex(innerY));

        PipelineStaticLocation afterInner = next(resolver.advance(innerY));
        assertEquals(location(List.of(ref("outer", "call-middle")), ref("middle", "after-inner")), afterInner);
        assertEquals(1, resolver.rootStepIndex(afterInner));

        PipelineStaticLocation outerC = next(resolver.advance(afterInner));
        assertEquals(location(List.of(), ref("outer", "c")), outerC);
        assertEquals(PipelineCompositionResolver.ResolvedContinuation.RootTerminal.INSTANCE, resolver.advance(outerC));
    }

    @Test
    void rejectsLocationWhoseInvocationFramesDoNotMatchTheCompiledRoute() {
        PipelineCompositionResolver resolver = new PipelineCompositionResolver(composition());
        PipelineStaticLocation invalid = location(List.of(ref("outer", "a")), ref("middle", "call-inner"));

        assertThrows(IllegalArgumentException.class, () -> resolver.advance(invalid));
    }

    @Test
    void acceptsFiniteDirectSelfRecursiveCompositionDescriptor() {
        PipelineCompositionDescriptor descriptor = new PipelineCompositionDescriptor("outer", List.of(
            definition("outer", List.of(invocation(0, "again", "outer")),
                continuation("again", PipelineCompositionContinuationKind.ROOT_TERMINAL, ""))));

        assertEquals("outer", descriptor.definition("outer").definitionId());
    }

    @Test
    void rejectsMutuallyRecursiveCompositionDescriptors() {
        IllegalArgumentException failure = assertThrows(
            IllegalArgumentException.class,
            () -> new PipelineCompositionDescriptor("outer", List.of(
                definition("outer", List.of(invocation(0, "call-inner", "inner")),
                    continuation("call-inner", PipelineCompositionContinuationKind.ROOT_TERMINAL, "")),
                definition("inner", List.of(invocation(0, "call-outer", "outer")),
                    continuation("call-outer", PipelineCompositionContinuationKind.RETURN, "")))));

        assertEquals("composition definitions must not contain cycles: outer", failure.getMessage());
    }

    @Test
    void rejectsDefinitionsUnreachableFromTheRoot() {
        IllegalArgumentException failure = assertThrows(
            IllegalArgumentException.class,
            () -> new PipelineCompositionDescriptor("outer", List.of(
                definition("outer", List.of(direct(0, "done")),
                    continuation("done", PipelineCompositionContinuationKind.ROOT_TERMINAL, "")),
                definition("unused", List.of(direct(0, "unused-step")),
                    continuation("unused-step", PipelineCompositionContinuationKind.RETURN, "")))));

        assertEquals("composition definition is unreachable from root: unused", failure.getMessage());
    }

    @Test
    void rejectsNonRootDefinitionWithoutReturnContinuation() {
        IllegalArgumentException failure = assertThrows(
            IllegalArgumentException.class,
            () -> new PipelineCompositionDescriptor("outer", List.of(
                definition("outer", List.of(invocation(0, "call-inner", "inner")),
                    continuation("call-inner", PipelineCompositionContinuationKind.ROOT_TERMINAL, "")),
                definition("inner", List.of(direct(0, "done")),
                    continuation("done", PipelineCompositionContinuationKind.ROOT_TERMINAL, "")))));

        assertEquals(
            "composition definition 'inner' must end with RETURN continuation",
            failure.getMessage());
    }

    @Test
    void rejectsTerminalNextLocalBeforeDescriptorMaterialization() {
        IllegalArgumentException failure = assertThrows(
            IllegalArgumentException.class,
            () -> new PipelineCompositionDescriptor("outer", List.of(
                definition("outer", List.of(invocation(0, "call-inner", "inner")),
                    continuation("call-inner", PipelineCompositionContinuationKind.ROOT_TERMINAL, "")),
                definition("inner", List.of(direct(0, "done")),
                    continuation("done", PipelineCompositionContinuationKind.NEXT_LOCAL, "done")))));

        assertEquals("terminal definition node cannot have NEXT_LOCAL continuation", failure.getMessage());
    }

    private static PipelineStaticLocation next(PipelineCompositionResolver.ResolvedContinuation continuation) {
        return assertInstanceOf(PipelineCompositionResolver.ResolvedContinuation.Next.class, continuation).location();
    }

    private static PipelineStaticLocation location(
        List<PipelineStaticLocation.NodeRef> frames,
        PipelineStaticLocation.NodeRef current
    ) {
        return new PipelineStaticLocation(frames, current);
    }

    private static PipelineStaticLocation.NodeRef ref(String definition, String node) {
        return new PipelineStaticLocation.NodeRef(definition, node);
    }

    private static PipelineCompositionDescriptor composition() {
        return new PipelineCompositionDescriptor("outer", List.of(
            definition("outer", List.of(
                direct(0, "a"), invocation(1, "call-middle", "middle"), direct(2, "c")),
                continuation("a", PipelineCompositionContinuationKind.NEXT_LOCAL, "call-middle"),
                continuation("call-middle", PipelineCompositionContinuationKind.NEXT_LOCAL, "c"),
                continuation("c", PipelineCompositionContinuationKind.ROOT_TERMINAL, "")),
            definition("middle", List.of(invocation(0, "call-inner", "inner"), direct(1, "after-inner")),
                continuation("call-inner", PipelineCompositionContinuationKind.NEXT_LOCAL, "after-inner"),
                continuation("after-inner", PipelineCompositionContinuationKind.RETURN, "")),
            definition("inner", List.of(direct(0, "x"), direct(1, "await"), direct(2, "y")),
                continuation("x", PipelineCompositionContinuationKind.NEXT_LOCAL, "await"),
                continuation("await", PipelineCompositionContinuationKind.NEXT_LOCAL, "y"),
                continuation("y", PipelineCompositionContinuationKind.RETURN, ""))));
    }

    private static PipelineCompositionDefinition definition(
        String id,
        List<PipelineCompositionNode> nodes,
        PipelineCompositionContinuation... continuations
    ) {
        return new PipelineCompositionDefinition(id, id + "-hash", "Value", "Value", nodes, List.of(continuations));
    }

    private static PipelineCompositionNode direct(int index, String id) {
        return new PipelineCompositionNode(index, id, PipelineCompositionNode.DIRECT, "Value", "Value", "ONE_TO_ONE", "");
    }

    private static PipelineCompositionNode invocation(int index, String id, String target) {
        return new PipelineCompositionNode(index, id, PipelineCompositionNode.INVOCATION, "Value", "Value", "ONE_TO_ONE", target);
    }

    private static PipelineCompositionContinuation continuation(
        String node,
        PipelineCompositionContinuationKind kind,
        String next
    ) {
        return new PipelineCompositionContinuation(node, kind, next);
    }
}
