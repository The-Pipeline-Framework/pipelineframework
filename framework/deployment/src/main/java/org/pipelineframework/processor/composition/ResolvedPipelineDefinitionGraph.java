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

import java.util.AbstractList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.RandomAccess;
import org.pipelineframework.config.CardinalitySemantics;

/**
 * Immutable compiler result for a statically resolved, acyclic definition graph.
 *
 * <p>This is deliberately not a release descriptor. Release-level artifact and digest pinning
 * remains the responsibility of the existing generated contract and release machinery.
 */
public record ResolvedPipelineDefinitionGraph(
    PipelineDefinition root,
    Map<PipelineReference, PipelineDefinition> definitions,
    CardinalitySemantics rootCardinality,
    List<PipelineInvocationBinding> invocationBindings,
    List<PipelineContinuationRoute> continuationRoutes
) {

    public ResolvedPipelineDefinitionGraph {
        root = Objects.requireNonNull(root, "root must not be null");
        definitions = Collections.unmodifiableMap(new LinkedHashMap<>(
            Objects.requireNonNull(definitions, "definitions must not be null")));
        if (!definitions.containsKey(root.reference())) {
            throw new IllegalArgumentException("Resolved graph must contain the root definition");
        }
        rootCardinality = Objects.requireNonNull(rootCardinality, "rootCardinality must not be null");
        invocationBindings = List.copyOf(Objects.requireNonNull(
            invocationBindings,
            "invocationBindings must not be null"));
        continuationRoutes = new ContinuationRouteIndex(Objects.requireNonNull(
            continuationRoutes,
            "continuationRoutes must not be null"));
    }

    public Optional<CompiledPipelineLocation> continuationAfter(CompiledPipelineLocation location) {
        Objects.requireNonNull(location, "location must not be null");
        return ((ContinuationRouteIndex) continuationRoutes).continuationAfter(location);
    }

    private static final class ContinuationRouteIndex extends AbstractList<PipelineContinuationRoute>
        implements RandomAccess {

        private final List<PipelineContinuationRoute> routes;
        private final Map<CompiledPipelineLocation, PipelineContinuationRoute> routesByCurrent;

        private ContinuationRouteIndex(List<PipelineContinuationRoute> routes) {
            this.routes = List.copyOf(routes);
            Map<CompiledPipelineLocation, PipelineContinuationRoute> indexed = new LinkedHashMap<>();
            for (PipelineContinuationRoute route : this.routes) {
                PipelineContinuationRoute duplicate = indexed.putIfAbsent(route.current(), route);
                if (duplicate != null) {
                    throw new IllegalArgumentException(
                        "Duplicate continuation route for compiled location: " + route.current().display());
                }
            }
            this.routesByCurrent = Collections.unmodifiableMap(indexed);
        }

        private Optional<CompiledPipelineLocation> continuationAfter(CompiledPipelineLocation location) {
            return Optional.ofNullable(routesByCurrent.get(location)).flatMap(PipelineContinuationRoute::next);
        }

        @Override
        public PipelineContinuationRoute get(int index) {
            return routes.get(index);
        }

        @Override
        public int size() {
            return routes.size();
        }
    }
}
