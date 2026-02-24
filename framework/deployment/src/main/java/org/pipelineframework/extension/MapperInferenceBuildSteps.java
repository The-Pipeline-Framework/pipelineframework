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
import org.jboss.jandex.Type;
import org.jboss.logging.Logger;
import org.pipelineframework.extension.MapperInferenceEngine.MapperRegistry;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.net.URL;
import java.util.ArrayList;
import java.util.Set;
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
    private static final DotName UNI = DotName.createSimple("io.smallrye.mutiny.Uni");
    private static final DotName MULTI = DotName.createSimple("io.smallrye.mutiny.Multi");
    private static final DotName COLLECTION = DotName.createSimple("java.util.Collection");
    private static final DotName LIST = DotName.createSimple("java.util.List");
    private static final DotName SET = DotName.createSimple("java.util.Set");
    private static final DotName QUEUE = DotName.createSimple("java.util.Queue");
    private static final DotName DEQUE = DotName.createSimple("java.util.Deque");
    private static final DotName SORTED_SET = DotName.createSimple("java.util.SortedSet");
    private static final DotName MAP = DotName.createSimple("java.util.Map");
    private static final DotName STREAM = DotName.createSimple("java.util.stream.Stream");
    private static final DotName COMPLETION_STAGE = DotName.createSimple("java.util.concurrent.CompletionStage");
    private static final Set<String> PLATFORM_PREFIXES = Set.of(
            "java.",
            "javax.",
            "jakarta.",
            "sun.",
            "com.sun.",
            "kotlin.",
            "org.springframework.",
            "org.slf4j.",
            "com.fasterxml.",
            "org.jboss.");
    private static final Set<DotName> WRAPPER_TYPES = Set.of(
            UNI,
            MULTI,
            STREAM,
            COMPLETION_STAGE,
            COLLECTION,
            LIST,
            SET,
            QUEUE,
            DEQUE,
            SORTED_SET);

    /**
     * Local data carrier for step definition metadata parsed from classpath resources.
     * <p>
     * This is a plain record used only for local data transfer within this build step class.
     * It is NOT a Quarkus build item and should not be confused with StepDefinitionBuildItem.
     *
     * @param stepName the fully qualified name of the step service class
     * @param domainIn DotName of the input domain type (E_in), or null if not specified
     * @param domainOut DotName of the output domain type (E_out), or null if not specified
     * @param cardinality the cardinality of the step; empty string if not specified
     */
    private record StepDefinition(String stepName, DotName domainIn, DotName domainOut, String cardinality) {
    }

    /**
     * Load pipeline step definitions from classpath resources at META-INF/pipeline/step-definitions.txt.
     *
     * <p>Parses each non-empty, non-comment line using the format:
     * stepName|domainIn|domainOut|cardinality (all four fields required). Empty domain fields are treated as absent.</p>
     *
     * @return a list of StepDefinition records representing the parsed step definitions
     * @throws IOException if an I/O error occurs while reading resources or if a step definition line is malformed
     */
    private List<StepDefinition> readStepDefinitions() throws IOException {
        List<StepDefinition> stepDefinitions = new ArrayList<>();

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
                    if (parts.length < 4) {
                        throw new IOException(String.format(
                                "Malformed step definition at %s:%d. Expected format: stepName|domainIn|domainOut|cardinality. Line: '%s'",
                                url, lineNumber, line));
                    }

                    String stepName = parts[0].trim();
                    String domainIn = parts[1].trim();
                    String domainOut = parts[2].trim();
                    String cardinality = parts[3].trim();

                    DotName domainInDotName = domainIn.isEmpty() ? null : DotName.createSimple(domainIn);
                    DotName domainOutDotName = domainOut.isEmpty() ? null : DotName.createSimple(domainOut);

                    stepDefinitions.add(new StepDefinition(
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
            List<OperatorBuildItem> operators,
            BuildProducer<MapperRegistryBuildItem> mapperRegistry) {

        LOG.debugf("Building mapper registry from Jandex index");

        IndexView index = combinedIndex.getIndex();
        List<StepDefinition> stepDefinitions = readStepDefinitions(operators, index);

        LOG.debugf("Read %d step definitions", stepDefinitions.size());

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

    // Convenience overload used by unit tests that call this class directly.
    void buildMapperRegistry(
            CombinedIndexBuildItem combinedIndex,
            BuildProducer<MapperRegistryBuildItem> mapperRegistry) {
        buildMapperRegistry(combinedIndex, List.of(), mapperRegistry);
    }

    private List<StepDefinition> readStepDefinitions(List<OperatorBuildItem> operators, IndexView index) {
        if (operators != null && !operators.isEmpty()) {
            List<StepDefinition> stepDefinitions = deriveStepDefinitionsFromOperators(operators, index);
            LOG.debugf("Derived %d step definitions from OperatorBuildItem list", stepDefinitions.size());
            return stepDefinitions;
        }
        try {
            return readStepDefinitions();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read step definitions", e);
        }
    }

    private List<StepDefinition> deriveStepDefinitionsFromOperators(List<OperatorBuildItem> operators, IndexView index) {
        List<StepDefinition> stepDefinitions = new ArrayList<>();
        for (OperatorBuildItem operator : operators) {
            DotName domainIn = resolveDomainCandidate(unwrapPayloadType(operator.inputType()), index);
            DotName domainOut = resolveDomainCandidate(extractNormalizedOutputType(operator.normalizedReturnType(), index), index);
            stepDefinitions.add(new StepDefinition(
                    operator.step().name(),
                    domainIn,
                    domainOut,
                    cardinalityFromNormalizedType(operator.normalizedReturnType())));
        }
        return stepDefinitions;
    }

    private DotName resolveDomainCandidate(Type type, IndexView index) {
        if (type == null || type.kind() == Type.Kind.VOID || type.name() == null) {
            return null;
        }
        DotName name = type.name();
        String fqcn = name.toString();
        for (String prefix : PLATFORM_PREFIXES) {
            if (fqcn.startsWith(prefix)) {
                return null;
            }
        }
        if (index.getClassByName(name) == null) {
            return null;
        }
        return name;
    }

    /**
     * Extracts the domain output type from normalized return metadata.
     * Supports single-argument wrappers and Map<K, V> (value type).
     */
    private Type extractNormalizedOutputType(Type normalizedReturnType, IndexView index) {
        if (normalizedReturnType == null || normalizedReturnType.kind() != Type.Kind.PARAMETERIZED_TYPE) {
            return normalizedReturnType;
        }
        List<Type> args = normalizedReturnType.asParameterizedType().arguments();
        if (args.size() == 1) {
            return args.get(0);
        }
        DotName raw = normalizedReturnType.asParameterizedType().name();
        if (args.size() == 2 && isMapLike(raw, index)) {
            return args.get(1);
        }
        throw new IllegalArgumentException("extractNormalizedOutputType expects a single-argument wrapper or Map<K,V>; got "
                + normalizedReturnType);
    }

    private Type unwrapPayloadType(Type type) {
        if (type == null || type.kind() != Type.Kind.PARAMETERIZED_TYPE) {
            return type;
        }
        DotName raw = type.name();
        if (!isWrapper(raw)) {
            return type;
        }
        List<Type> args = type.asParameterizedType().arguments();
        return args.isEmpty() ? type : unwrapPayloadType(args.get(0));
    }

    private boolean isWrapper(DotName rawType) {
        return WRAPPER_TYPES.contains(rawType);
    }

    private boolean isMapLike(DotName rawType, IndexView index) {
        if (rawType == null) {
            return false;
        }
        if (MAP.equals(rawType)) {
            return true;
        }
        ClassInfo classInfo = index.getClassByName(rawType);
        if (classInfo == null) {
            return false;
        }
        for (Type interfaceType : classInfo.interfaceTypes()) {
            if (MAP.equals(interfaceType.name())) {
                return true;
            }
        }
        Type superType = classInfo.superClassType();
        return superType != null && isMapLike(superType.name(), index);
    }

    private String cardinalityFromNormalizedType(Type normalizedReturnType) {
        if (normalizedReturnType == null || normalizedReturnType.name() == null) {
            return "";
        }
        DotName raw = normalizedReturnType.name();
        if (MULTI.equals(raw)) {
            return "MULTI";
        }
        if (UNI.equals(raw)) {
            return "UNI";
        }
        return "";
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
            List<StepDefinition> stepDefinitions,
            MapperRegistry registry,
            IndexView index) {

        List<String> validationErrors = new ArrayList<>();

        for (StepDefinition step : stepDefinitions) {
            // Validate input domain mapper (outbound mapper: entity -> DTO -> proto)
            if (step.domainIn() != null) {
                if (!registry.domainToMapper().containsKey(step.domainIn())) {
                    // Check if the domain type exists in the index
                    ClassInfo domainClass = index.getClassByName(step.domainIn());
                    if (domainClass == null) {
                        validationErrors.add(String.format(
                                "Step '%s' references input domain type '%s' that is not in the Jandex index. " +
                                "Ensure the class is indexed by Quarkus.",
                                step.stepName(), step.domainIn()));
                    } else {
                        validationErrors.add(String.format(
                                "Step '%s' has no outbound mapper for input domain type '%s'. " +
                                "PER REQUIREMENTS: All mappers MUST be resolved at build time. " +
                                "Add a Mapper implementation: Mapper<?, ?, %s>",
                                step.stepName(), step.domainIn(), step.domainIn()));
                    }
                }
            }

            // Validate output domain mapper (inbound mapper: proto -> DTO -> entity)
            if (step.domainOut() != null) {
                if (!registry.domainToMapper().containsKey(step.domainOut())) {
                    ClassInfo domainClass = index.getClassByName(step.domainOut());
                    if (domainClass == null) {
                        validationErrors.add(String.format(
                                "Step '%s' references output domain type '%s' that is not in the Jandex index. " +
                                "Ensure the class is indexed by Quarkus.",
                                step.stepName(), step.domainOut()));
                    } else {
                        validationErrors.add(String.format(
                                "Step '%s' has no inbound mapper for output domain type '%s'. " +
                                "PER REQUIREMENTS: All mappers MUST be resolved at build time. " +
                                "Add a Mapper implementation: Mapper<?, ?, %s>",
                                step.stepName(), step.domainOut(), step.domainOut()));
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
