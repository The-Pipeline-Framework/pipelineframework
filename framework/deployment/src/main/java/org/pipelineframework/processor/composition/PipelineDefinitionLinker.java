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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.pipelineframework.config.CardinalitySemantics;

/**
 * Links local compiler definitions into a source-neutral, statically expanded composition graph.
 */
public final class PipelineDefinitionLinker {

    private final PipelineDefinitionResolver resolver;

    public PipelineDefinitionLinker(PipelineDefinitionResolver resolver) {
        this.resolver = Objects.requireNonNull(resolver, "resolver must not be null");
    }

    public ResolvedPipelineDefinitionGraph link(PipelineDefinition root) {
        Objects.requireNonNull(root, "root must not be null");
        Map<PipelineReference, PipelineDefinition> definitions = new LinkedHashMap<>();
        Map<PipelineReference, CardinalitySemantics> cardinalities = new LinkedHashMap<>();
        CardinalitySemantics rootCardinality = resolveCardinality(
            root,
            definitions,
            cardinalities,
            new ArrayDeque<>());

        List<PipelineInvocationBinding> bindings = new ArrayList<>();
        List<CompiledPipelineLocation> executableLocations = new ArrayList<>();
        expand(root, List.of(), definitions, cardinalities, bindings, executableLocations);

        List<PipelineContinuationRoute> routes = new ArrayList<>();
        for (int index = 0; index < executableLocations.size(); index++) {
            routes.add(new PipelineContinuationRoute(
                executableLocations.get(index),
                index + 1 < executableLocations.size()
                    ? java.util.Optional.of(executableLocations.get(index + 1))
                    : java.util.Optional.empty()));
        }
        return new ResolvedPipelineDefinitionGraph(root, definitions, rootCardinality, bindings, routes);
    }

    private CardinalitySemantics resolveCardinality(
        PipelineDefinition definition,
        Map<PipelineReference, PipelineDefinition> definitions,
        Map<PipelineReference, CardinalitySemantics> cardinalities,
        Deque<PipelineReference> stack
    ) {
        PipelineReference reference = definition.reference();
        CardinalitySemantics cached = cardinalities.get(reference);
        if (cached != null) {
            return cached;
        }
        if (stack.contains(reference)) {
            throw new IllegalArgumentException(
                "Static pipeline definition cycle is not supported: " + cycleDescription(stack, reference));
        }
        PipelineDefinition existing = definitions.putIfAbsent(reference, definition);
        if (existing != null && !existing.equals(definition)) {
            throw new IllegalArgumentException("Conflicting definitions for reference: " + reference.logicalId());
        }

        stack.addLast(reference);
        try {
            List<CardinalitySemantics> stepCardinalities = new ArrayList<>();
            for (PipelineDefinitionStep step : definition.steps()) {
                if (step.directCardinality().isPresent()) {
                    stepCardinalities.add(step.directCardinality().orElseThrow());
                    continue;
                }
                PipelineReference targetReference = step.pipelineReference().orElseThrow();
                PipelineDefinition target = resolveDefinition(targetReference);
                validateResolvedReference(targetReference, target);
                validateInvocationContract(step, target);
                stepCardinalities.add(resolveCardinality(target, definitions, cardinalities, stack));
            }
            CardinalitySemantics resolved = CardinalitySemantics.compose(stepCardinalities);
            cardinalities.put(reference, resolved);
            return resolved;
        } finally {
            stack.removeLast();
        }
    }

    private PipelineDefinition resolveDefinition(PipelineReference reference) {
        return resolver.resolve(reference).orElseThrow(() -> new IllegalArgumentException(
            "Static pipeline reference could not be resolved: " + reference.logicalId()));
    }

    private static void validateInvocationContract(PipelineDefinitionStep invocation, PipelineDefinition target) {
        if (!invocation.inputContractId().equals(target.inputContractId())) {
            throw new IllegalArgumentException(
                "Pipeline reference " + target.reference().logicalId() + " input contract does not match callsite "
                    + invocation.localStepId());
        }
        if (!invocation.outputContractId().equals(target.outputContractId())) {
            throw new IllegalArgumentException(
                "Pipeline reference " + target.reference().logicalId() + " output contract does not match callsite "
                    + invocation.localStepId());
        }
    }

    private static void validateResolvedReference(PipelineReference requested, PipelineDefinition resolved) {
        if (!requested.equals(resolved.reference())) {
            throw new IllegalArgumentException(
                "Resolved definition reference does not match requested reference: " + requested.logicalId());
        }
    }

    private static String cycleDescription(Deque<PipelineReference> stack, PipelineReference repeated) {
        List<String> cycle = new ArrayList<>();
        boolean include = false;
        for (PipelineReference reference : stack) {
            if (reference.equals(repeated)) {
                include = true;
            }
            if (include) {
                cycle.add(reference.logicalId());
            }
        }
        cycle.add(repeated.logicalId());
        return String.join(" -> ", cycle);
    }

    private static void expand(
        PipelineDefinition definition,
        List<DefinitionLocalLocation> invocationPath,
        Map<PipelineReference, PipelineDefinition> definitions,
        Map<PipelineReference, CardinalitySemantics> cardinalities,
        List<PipelineInvocationBinding> bindings,
        List<CompiledPipelineLocation> executableLocations
    ) {
        for (PipelineDefinitionStep step : definition.steps()) {
            DefinitionLocalLocation localLocation = new DefinitionLocalLocation(
                definition.reference(),
                step.localStepId());
            CompiledPipelineLocation compiledLocation = new CompiledPipelineLocation(invocationPath, localLocation);
            if (step.directCardinality().isPresent()) {
                executableLocations.add(compiledLocation);
                continue;
            }
            PipelineReference targetReference = step.pipelineReference().orElseThrow();
            PipelineDefinition target = definitions.get(targetReference);
            if (target == null) {
                throw new IllegalStateException("Resolved definition is missing: " + targetReference.logicalId());
            }
            CardinalitySemantics targetCardinality = cardinalities.get(targetReference);
            if (targetCardinality == null) {
                throw new IllegalStateException("Resolved cardinality is missing: " + targetReference.logicalId());
            }
            List<DefinitionLocalLocation> childPath = new ArrayList<>(invocationPath);
            childPath.add(localLocation);
            bindings.add(new PipelineInvocationBinding(
                compiledLocation,
                targetReference,
                targetCardinality,
                target.steps().stream().map(child -> new CompiledPipelineLocation(
                    childPath,
                    new DefinitionLocalLocation(target.reference(), child.localStepId()))).toList()));
            expand(target, childPath, definitions, cardinalities, bindings, executableLocations);
        }
    }
}
