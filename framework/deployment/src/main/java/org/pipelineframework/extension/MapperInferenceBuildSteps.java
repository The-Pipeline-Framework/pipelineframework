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

package org.pipelineframework.extension;

import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.smallrye.common.annotation.Experimental;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;
import org.jboss.logging.Logger;
import org.pipelineframework.extension.MapperInferenceEngine.MapperRegistry;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * Build steps for mapper inference using the Jandex index.
 * <p>
 * This class provides the build step that consumes the CombinedIndexBuildItem
 * and produces the MapperRegistryBuildItem containing all resolved mapper
 * assignments.
 * <p>
 * PER REQUIREMENTS:
 * <ul>
 *   <li>All mappers MUST be resolved at build time - zero runtime resolution</li>
 *   <li>Use Jandex index for full classpath visibility</li>
 *   <li>Fail fast on ambiguity</li>
 *   <li>No fallback. No silent transport bypass.</li>
 * </ul>
 */
@Experimental("Mapper inference based on Jandex index")
public class MapperInferenceBuildSteps {

    private static final Logger LOG = Logger.getLogger(MapperInferenceBuildSteps.class);
    private static final String STEP_DEFINITIONS_FILE = "META-INF/pipeline/step-definitions.txt";

    /**
     * Load pipeline step definitions from classpath resources at META-INF/pipeline/step-definitions.txt.
     *
     * <p>Parses each non-empty, non-comment line using the format:
     * stepName|domainIn|domainOut|cardinality (cardinality is optional). Empty domain fields are treated as absent.</p>
     *
     * @return a list of StepDefinitionBuildItem representing the parsed step definitions
     * @throws IOException if an I/O error occurs while reading resources or if a step definition line is malformed
     */
    private List<StepDefinitionBuildItem> readStepDefinitions() throws IOException {
        List<StepDefinitionBuildItem> stepDefinitions = new ArrayList<>();

        Enumeration<URL> resources = Thread.currentThread().getContextClassLoader()
                .getResources(STEP_DEFINITIONS_FILE);

        while (resources.hasMoreElements()) {
            URL url = resources.nextElement();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), StandardCharsets.UTF_8))) {
                String line;
                int lineNumber = 0;
                while ((line = reader.readLine()) != null) {
                    lineNumber++;
                    line = line.trim();
                    if (line.isEmpty() || line.startsWith("#")) {
                        continue;
                    }

                    String[] parts = line.split("\\|");
                    if (parts.length < 3) {
                        throw new IOException(String.format(
                                "Malformed step definition at %s:%d. Expected format: stepName|domainIn|domainOut[|cardinality]. Line: '%s'",
                                url, lineNumber, line));
                    }

                    String stepName = parts[0].trim();
                    String domainIn = parts[1].trim();
                    String domainOut = parts[2].trim();
                    String cardinality = parts.length > 3 ? parts[3].trim() : "";

                    DotName domainInDotName = domainIn.isEmpty() ? null : DotName.createSimple(domainIn);
                    DotName domainOutDotName = domainOut.isEmpty() ? null : DotName.createSimple(domainOut);

                    stepDefinitions.add(new StepDefinitionBuildItem(
                            stepName, domainInDotName, domainOutDotName, cardinality));
                }
            }
        }

        return stepDefinitions;
    }

    /**
     * Builds a MapperRegistry from the application's Jandex index and emits it as a MapperRegistryBuildItem.
     *
     * <p>Reads step definitions, discovers Mapper implementations from the index, validates that each pipeline
     * step has matching mappers for its input/output domain types, and produces a MapperRegistryBuildItem for
     * use by downstream build steps.</p>
     *
     * @param combinedIndex the combined Jandex index of application and dependency classes
     * @param mapperRegistry producer used to emit the resulting MapperRegistryBuildItem
     */
    @BuildStep
    void buildMapperRegistry(
            CombinedIndexBuildItem combinedIndex,
            BuildProducer<MapperRegistryBuildItem> mapperRegistry) {

        LOG.debugf("Building mapper registry from Jandex index");

        List<StepDefinitionBuildItem> stepDefinitions;
        try {
            stepDefinitions = readStepDefinitions();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read step definitions", e);
        }

        LOG.debugf("Read %d step definitions", stepDefinitions.size());

        IndexView index = combinedIndex.getIndex();
        MapperInferenceEngine engine = new MapperInferenceEngine(index);

        // Build the mapper registry from all discovered mappers
        MapperRegistry registry = engine.buildRegistry();

        LOG.debugf("Mapper registry built with %d mappers", registry.domainToMapper().size());

        // Log discovered mappers for debugging
        if (LOG.isDebugEnabled()) {
            for (DotName domainType : registry.domainToMapper().keySet()) {
                ClassInfo mapper = registry.domainToMapper().get(domainType);
                LOG.debugf("  Mapper for %s: %s", domainType, mapper.name());
            }
        }

        // Validate that all steps have mappers for their domain types
        validateStepMappers(stepDefinitions, registry, index);

        // Produce the registry build item
        mapperRegistry.produce(new MapperRegistryBuildItem(
                registry.domainToMapper(),
                registry.mapperToDomain()));
    }

    /**
     * Ensure every pipeline step has mappers for its input and output domain types.
     *
     * <p>Per requirements, fails fast on any missing mapper and reports whether the domain type
     * itself is absent from the Jandex index or present but missing a mapper implementation.</p>
     *
     * @param stepDefinitions the step definitions to validate
     * @param registry the mapper registry mapping domain types to discovered mappers
     * @param index the Jandex index used to determine whether a domain type is present in the application index
     * @throws IllegalStateException if one or more steps are missing required mappers or referenced domain types are not indexed
     */
    private void validateStepMappers(
            List<StepDefinitionBuildItem> stepDefinitions,
            MapperRegistry registry,
            IndexView index) {

        List<String> validationErrors = new ArrayList<>();

        for (StepDefinitionBuildItem step : stepDefinitions) {
            // Validate input domain mapper (outbound mapper: entity -> DTO -> proto)
            if (step.getDomainIn() != null) {
                if (!registry.domainToMapper().containsKey(step.getDomainIn())) {
                    // Check if the domain type exists in the index
                    ClassInfo domainClass = index.getClassByName(step.getDomainIn());
                    if (domainClass == null) {
                        validationErrors.add(String.format(
                                "Step '%s' references input domain type '%s' that is not in the Jandex index. " +
                                "Ensure the class is indexed by Quarkus.",
                                step.getStepName(), step.getDomainIn()));
                    } else {
                        validationErrors.add(String.format(
                                "Step '%s' has no outbound mapper for input domain type '%s'. " +
                                "PER REQUIREMENTS: All mappers MUST be resolved at build time. " +
                                "Add a Mapper implementation: Mapper<?, ?, %s>",
                                step.getStepName(), step.getDomainIn(), step.getDomainIn()));
                    }
                }
            }

            // Validate output domain mapper (inbound mapper: proto -> DTO -> entity)
            if (step.getDomainOut() != null) {
                if (!registry.domainToMapper().containsKey(step.getDomainOut())) {
                    ClassInfo domainClass = index.getClassByName(step.getDomainOut());
                    if (domainClass == null) {
                        validationErrors.add(String.format(
                                "Step '%s' references output domain type '%s' that is not in the Jandex index. " +
                                "Ensure the class is indexed by Quarkus.",
                                step.getStepName(), step.getDomainOut()));
                    } else {
                        validationErrors.add(String.format(
                                "Step '%s' has no inbound mapper for output domain type '%s'. " +
                                "PER REQUIREMENTS: All mappers MUST be resolved at build time. " +
                                "Add a Mapper implementation: Mapper<?, ?, %s>",
                                step.getStepName(), step.getDomainOut(), step.getDomainOut()));
                    }
                }
            }
        }

        // Fail fast if any validation errors
        if (!validationErrors.isEmpty()) {
            throw new IllegalStateException("Mapper validation failed for pipeline steps:\n" +
                    String.join("\n", validationErrors));
        }
    }
}