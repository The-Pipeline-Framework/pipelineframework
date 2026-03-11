/*
 * Copyright (c) 2023-2025 Mariano Barcia
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

package org.pipelineframework;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import jakarta.enterprise.context.ApplicationScoped;

import org.jboss.logging.Logger;
import org.pipelineframework.config.pipeline.PipelineOrderResourceLoader;

@ApplicationScoped
class PipelineStepOrderer {

    private static final Logger logger = Logger.getLogger(PipelineStepOrderer.class);

    List<Object> orderSteps(List<Object> steps) {
        Optional<List<String>> resourceOrder = PipelineOrderResourceLoader.loadOrder();
        if (resourceOrder.isEmpty()) {
            if (!PipelineOrderResourceLoader.requiresOrder()) {
                logger.debug("Pipeline order metadata not found and not required; preserving existing step order.");
                return steps;
            }
            throw new IllegalStateException(
                "Pipeline order metadata not found. Ensure META-INF/pipeline/order.json is generated at build time.");
        }
        List<String> filteredPipelineOrder = resourceOrder.get();
        if (filteredPipelineOrder.isEmpty()) {
            if (!PipelineOrderResourceLoader.requiresOrder()) {
                logger.debug("Pipeline order metadata is empty and not required; preserving existing step order.");
                return steps;
            }
            throw new IllegalStateException(
                "Pipeline order metadata is empty. Ensure pipeline.yaml defines steps for order generation.");
        }
        return applyConfiguredOrder(steps, filteredPipelineOrder);
    }

    List<Object> applyConfiguredOrder(List<Object> steps, List<String> filteredPipelineOrder) {
        if (filteredPipelineOrder == null || filteredPipelineOrder.isEmpty()) {
            return steps;
        }

        Set<String> configuredNames = new HashSet<>(filteredPipelineOrder);
        // A partial generated order is unreliable: if any runtime step class is missing from the
        // metadata, preserve the original list to avoid applying an incomplete order to the pipeline.
        boolean hasUnconfiguredSteps = steps.stream()
            .map(step -> step != null ? step.getClass().getName() : null)
            .anyMatch(name -> name != null && !configuredNames.contains(name));
        if (hasUnconfiguredSteps) {
            logger.debug("Pipeline order configured, but step list contains unconfigured entries; preserving existing order.");
            return steps;
        }

        Map<String, List<Object>> stepMap = new HashMap<>();
        for (Object step : steps) {
            if (step == null) {
                continue;
            }
            stepMap.computeIfAbsent(step.getClass().getName(), ignored -> new ArrayList<>()).add(step);
        }

        List<Object> orderedSteps = new ArrayList<>();
        Set<Object> addedSteps = Collections.newSetFromMap(new IdentityHashMap<>());
        for (String stepClassName : filteredPipelineOrder) {
            List<Object> stepInstances = stepMap.get(stepClassName);
            if (stepInstances != null && !stepInstances.isEmpty()) {
                Object step = stepInstances.remove(0);
                orderedSteps.add(step);
                addedSteps.add(step);
            } else {
                logger.warnf("Step class %s was specified in pipeline order but was not found in the available steps", stepClassName);
            }
        }

        for (Object step : steps) {
            if (!addedSteps.contains(step)) {
                logger.debugf("Adding step %s that wasn't specified in pipeline order", step.getClass().getName());
                orderedSteps.add(step);
            }
        }

        return orderedSteps;
    }
}
