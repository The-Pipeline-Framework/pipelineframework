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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.pipelineframework.config.CardinalitySemantics;
import org.pipelineframework.orchestrator.composition.PipelineCompositionContinuation;
import org.pipelineframework.orchestrator.composition.PipelineCompositionContinuationKind;
import org.pipelineframework.orchestrator.composition.PipelineCompositionDefinition;
import org.pipelineframework.orchestrator.composition.PipelineCompositionDescriptor;
import org.pipelineframework.orchestrator.composition.PipelineCompositionNode;

/** Projects the compiler-owned resolved definition graph into the pinned contract representation. */
public final class PipelineCompositionContractProjector {

    private static final Gson CANONICAL_GSON = new GsonBuilder().disableHtmlEscaping().create();

    public PipelineCompositionDescriptor project(ResolvedPipelineDefinitionGraph graph) {
        Objects.requireNonNull(graph, "graph must not be null");
        Map<PipelineReference, CardinalitySemantics> invocationCardinalities = invocationCardinalities(graph);
        List<PipelineCompositionDefinition> definitions = graph.definitions().values().stream()
            .map(definition -> projectDefinition(definition, graph.root().reference(), invocationCardinalities))
            .toList();
        return new PipelineCompositionDescriptor(graph.root().reference().logicalId(), definitions);
    }

    private PipelineCompositionDefinition projectDefinition(
        PipelineDefinition definition,
        PipelineReference rootReference,
        Map<PipelineReference, CardinalitySemantics> invocationCardinalities
    ) {
        List<PipelineCompositionNode> nodes = new ArrayList<>();
        for (int index = 0; index < definition.steps().size(); index++) {
            PipelineDefinitionStep step = definition.steps().get(index);
            nodes.add(projectNode(index, step, invocationCardinalities));
        }
        List<PipelineCompositionContinuation> continuations = continuations(definition, rootReference, nodes);
        return new PipelineCompositionDefinition(
            definition.reference().logicalId(),
            fingerprint(definition, nodes, continuations),
            definition.inputContractId(),
            definition.outputContractId(),
            nodes,
            continuations);
    }

    private PipelineCompositionNode projectNode(
        int index,
        PipelineDefinitionStep step,
        Map<PipelineReference, CardinalitySemantics> invocationCardinalities
    ) {
        if (step.directCardinality().isPresent()) {
            return new PipelineCompositionNode(
                index,
                step.localStepId(),
                PipelineCompositionNode.DIRECT,
                step.inputContractId(),
                step.outputContractId(),
                step.directCardinality().orElseThrow().name(),
                "");
        }
        PipelineReference target = step.pipelineReference().orElseThrow();
        CardinalitySemantics cardinality = invocationCardinalities.get(target);
        if (cardinality == null) {
            throw new IllegalStateException("Resolved invocation cardinality is missing for " + target.logicalId());
        }
        return new PipelineCompositionNode(
            index,
            step.localStepId(),
            PipelineCompositionNode.INVOCATION,
            step.inputContractId(),
            step.outputContractId(),
            cardinality.name(),
            target.logicalId());
    }

    private static List<PipelineCompositionContinuation> continuations(
        PipelineDefinition definition,
        PipelineReference rootReference,
        List<PipelineCompositionNode> nodes
    ) {
        List<PipelineCompositionContinuation> continuations = new ArrayList<>();
        for (int index = 0; index < nodes.size(); index++) {
            PipelineCompositionNode node = nodes.get(index);
            if (index + 1 < nodes.size()) {
                continuations.add(new PipelineCompositionContinuation(
                    node.nodeId(), PipelineCompositionContinuationKind.NEXT_LOCAL, nodes.get(index + 1).nodeId()));
            } else if (definition.reference().equals(rootReference)) {
                continuations.add(new PipelineCompositionContinuation(
                    node.nodeId(), PipelineCompositionContinuationKind.ROOT_TERMINAL, ""));
            } else {
                continuations.add(new PipelineCompositionContinuation(
                    node.nodeId(), PipelineCompositionContinuationKind.RETURN, ""));
            }
        }
        return continuations;
    }

    private static Map<PipelineReference, CardinalitySemantics> invocationCardinalities(
        ResolvedPipelineDefinitionGraph graph
    ) {
        Map<PipelineReference, CardinalitySemantics> result = new LinkedHashMap<>();
        for (PipelineInvocationBinding binding : graph.invocationBindings()) {
            CardinalitySemantics previous = result.putIfAbsent(binding.target(), binding.cardinality());
            if (previous != null && previous != binding.cardinality()) {
                throw new IllegalStateException("Invocation cardinality differs for " + binding.target().logicalId());
            }
        }
        return result;
    }

    private static String fingerprint(
        PipelineDefinition definition,
        List<PipelineCompositionNode> nodes,
        List<PipelineCompositionContinuation> continuations
    ) {
        Map<String, Object> canonical = new LinkedHashMap<>();
        canonical.put("definitionId", definition.reference().logicalId());
        canonical.put("inputContractId", definition.inputContractId());
        canonical.put("outputContractId", definition.outputContractId());
        canonical.put("nodes", nodes);
        canonical.put("continuations", continuations);
        return sha256(CANONICAL_GSON.toJson(canonical));
    }

    private static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte valueByte : digest) {
                result.append(String.format("%02x", valueByte));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 must be available", exception);
        }
    }
}
