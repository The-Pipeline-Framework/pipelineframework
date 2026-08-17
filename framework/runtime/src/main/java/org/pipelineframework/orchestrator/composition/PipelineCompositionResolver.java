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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Resolves structured static continuation solely from a pinned composition descriptor. */
public final class PipelineCompositionResolver {
    private final PipelineCompositionDescriptor descriptor;

    public PipelineCompositionResolver(PipelineCompositionDescriptor descriptor) {
        this.descriptor = Objects.requireNonNull(descriptor, "descriptor must not be null");
        if (!descriptor.present()) {
            throw new IllegalArgumentException("A composition resolver requires a composition descriptor");
        }
    }

    public ResolvedContinuation advance(PipelineStaticLocation location) {
        Objects.requireNonNull(location, "location must not be null");
        validate(location);
        return advance(location.invocationPath(), location.current());
    }

    public int rootStepIndex(PipelineStaticLocation location) {
        Objects.requireNonNull(location, "location must not be null");
        validate(location);
        PipelineStaticLocation.NodeRef rootNode = location.invocationPath().isEmpty()
            ? location.current()
            : location.invocationPath().getFirst();
        if (!descriptor.rootDefinitionId().equals(rootNode.definitionId())) {
            throw new IllegalArgumentException("Static location does not begin in the composition root");
        }
        return descriptor.definition(rootNode.definitionId()).node(rootNode.nodeId()).index();
    }

    private ResolvedContinuation advance(
        List<PipelineStaticLocation.NodeRef> invocationPath,
        PipelineStaticLocation.NodeRef current
    ) {
        PipelineCompositionDefinition definition = descriptor.definition(current.definitionId());
        PipelineCompositionContinuation continuation = definition.continuation(current.nodeId());
        if (continuation.kind() == PipelineCompositionContinuationKind.NEXT_LOCAL) {
            return new ResolvedContinuation.Next(new PipelineStaticLocation(
                invocationPath,
                new PipelineStaticLocation.NodeRef(definition.definitionId(), continuation.nextNodeId())));
        }
        if (continuation.kind() == PipelineCompositionContinuationKind.ROOT_TERMINAL) {
            if (!invocationPath.isEmpty() || !descriptor.rootDefinitionId().equals(definition.definitionId())) {
                throw new IllegalArgumentException("ROOT_TERMINAL continuation is only valid at the root definition");
            }
            return ResolvedContinuation.RootTerminal.INSTANCE;
        }
        if (invocationPath.isEmpty()) {
            throw new IllegalArgumentException("RETURN continuation has no invoking callsite");
        }
        List<PipelineStaticLocation.NodeRef> callerPath = new ArrayList<>(invocationPath);
        PipelineStaticLocation.NodeRef callsite = callerPath.removeLast();
        return advance(callerPath, callsite);
    }

    private void validate(PipelineStaticLocation location) {
        String expectedDefinition = descriptor.rootDefinitionId();
        for (PipelineStaticLocation.NodeRef frame : location.invocationPath()) {
            if (!expectedDefinition.equals(frame.definitionId())) {
                throw new IllegalArgumentException("Static invocation path does not follow the composition graph");
            }
            PipelineCompositionDefinition definition = descriptor.definition(expectedDefinition);
            PipelineCompositionNode node = definition.node(frame.nodeId());
            if (!node.invocation()) {
                throw new IllegalArgumentException("Static invocation path does not follow the composition graph");
            }
            expectedDefinition = node.targetDefinitionId();
        }
        PipelineCompositionDefinition currentDefinition = descriptor.definition(expectedDefinition);
        if (!expectedDefinition.equals(location.current().definitionId())) {
            throw new IllegalArgumentException("Static current node does not match the invocation path target");
        }
        currentDefinition.node(location.current().nodeId());
    }

    public sealed interface ResolvedContinuation permits ResolvedContinuation.Next, ResolvedContinuation.RootTerminal {
        record Next(PipelineStaticLocation location) implements ResolvedContinuation {
            public Next {
                location = Objects.requireNonNull(location, "location must not be null");
            }
        }

        enum RootTerminal implements ResolvedContinuation {
            INSTANCE
        }
    }
}
