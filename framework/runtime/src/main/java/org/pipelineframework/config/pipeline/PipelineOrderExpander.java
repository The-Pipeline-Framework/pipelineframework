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

package org.pipelineframework.config.pipeline;

import java.util.*;

/**
 * Expands a pipeline order by inserting aspect-driven side-effect client steps.
 */
public final class PipelineOrderExpander {

    private static final org.jboss.logging.Logger LOG = org.jboss.logging.Logger.getLogger(PipelineOrderExpander.class);

    private PipelineOrderExpander() {
    }

    /**
     * Expand a base pipeline order by inserting side-effect client steps from configured aspects.
     *
     * @param baseOrder the configured base order of step class names
     * @param config the pipeline YAML configuration
     * @param classLoader class loader used for optional type resolution
     * @return the expanded pipeline order
     */
    public static List<String> expand(List<String> baseOrder, PipelineYamlConfig config, ClassLoader classLoader) {
        if (baseOrder == null || baseOrder.isEmpty() || config == null) {
            return baseOrder;
        }

        if (containsSideEffectSteps(baseOrder)) {
            return baseOrder;
        }

        List<PipelineYamlStep> steps = config.steps();
        List<PipelineYamlAspect> aspects = config.aspects();
        if (aspects == null || aspects.isEmpty()) {
            return baseOrder;
        }

        List<PipelineYamlAspect> clientAspects = aspects.stream()
            .filter(PipelineOrderExpander::supportsClientSteps)
            .toList();
        if (clientAspects.isEmpty()) {
            return baseOrder;
        }

        String basePackage = resolveBasePackage(baseOrder, config.basePackage());

        List<String> expanded = new ArrayList<>();
        Set<String> insertedKeys = new HashSet<>();

        List<PipelineYamlAspect> beforeAspects = clientAspects.stream()
            .filter(aspect -> "BEFORE_STEP".equalsIgnoreCase(aspect.position()))
            .toList();
        List<PipelineYamlAspect> afterAspects = clientAspects.stream()
            .filter(aspect -> !"BEFORE_STEP".equalsIgnoreCase(aspect.position()))
            .toList();

        String clientSuffix = resolveClientStepSuffix(config);

        for (String stepClassName : baseOrder) {
            for (PipelineYamlAspect aspect : beforeAspects) {
                if (!matchesScope(aspect, stepClassName)) {
                    continue;
                }
                String typeName = resolveTypeForAspect(stepClassName, steps, classLoader, aspect.position());
                if (typeName == null || typeName.isBlank()) {
                    continue;
                }
                String key = aspect.name() + ":" + typeName;
                if (!insertedKeys.add(key)) {
                    continue;
                }
                String sideEffectStep = buildSideEffectClientStep(
                    stepClassName, basePackage, aspect.name(), typeName, clientSuffix);
                expanded.add(sideEffectStep);
                if (LOG.isDebugEnabled()) {
                    LOG.debugf("Inserted side-effect client step %s before %s", sideEffectStep, stepClassName);
                }
            }

            if (stepClassName != null && !stepClassName.isBlank()) {
                expanded.add(stepClassName);
            }

            for (PipelineYamlAspect aspect : afterAspects) {
                if (!matchesScope(aspect, stepClassName)) {
                    continue;
                }
                String typeName = resolveTypeForAspect(stepClassName, steps, classLoader, aspect.position());
                if (typeName == null || typeName.isBlank()) {
                    continue;
                }
                String key = aspect.name() + ":" + typeName;
                if (!insertedKeys.add(key)) {
                    continue;
                }
                String sideEffectStep = buildSideEffectClientStep(
                    stepClassName, basePackage, aspect.name(), typeName, clientSuffix);
                expanded.add(sideEffectStep);
                if (LOG.isDebugEnabled()) {
                    LOG.debugf("Inserted side-effect client step %s after %s", sideEffectStep, stepClassName);
                }
            }
        }

        if (classLoader != null && LOG.isDebugEnabled()) {
            for (String stepClassName : expanded) {
                if (stepClassName == null || stepClassName.isBlank()) {
                    continue;
                }
                try {
                    Class.forName(stepClassName, false, classLoader);
                } catch (ClassNotFoundException missing) {
                    LOG.warnf("Expanded pipeline order includes missing class %s", stepClassName);
                }
            }
        }
        return expanded;
    }

    private static boolean supportsClientSteps(PipelineYamlAspect aspect) {
        if (aspect == null || !aspect.enabled()) {
            return false;
        }
        return true;
    }

    private static boolean matchesScope(PipelineYamlAspect aspect, String stepClassName) {
        if (aspect == null) {
            return false;
        }
        String scope = aspect.scope();
        if (scope == null || scope.isBlank() || "GLOBAL".equalsIgnoreCase(scope)) {
            return true;
        }
        if (!"STEPS".equalsIgnoreCase(scope)) {
            return false;
        }
        List<String> targetSteps = aspect.targetSteps();
        if (targetSteps == null || targetSteps.isEmpty()) {
            return false;
        }
        String normalizedStep = normalizeStepToken(stepClassName);
        for (String target : targetSteps) {
            if (normalizedStep.equalsIgnoreCase(normalizeStepToken(target))) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsSideEffectSteps(List<String> baseOrder) {
        return baseOrder.stream().anyMatch(name -> name != null
            && (name.contains("SideEffectGrpcClientStep") || name.contains("SideEffectRestClientStep")));
    }

    private static String resolveTypeForAspect(
        String stepClassName,
        List<PipelineYamlStep> steps,
        ClassLoader classLoader,
        String position
    ) {
        if (stepClassName == null || stepClassName.isBlank()) {
            return null;
        }

        boolean useInputType = "BEFORE_STEP".equalsIgnoreCase(position);
        String reflected = resolveTypeFromClass(stepClassName, classLoader, useInputType);
        if (reflected != null && !reflected.isBlank()) {
            return reflected;
        }

        if (steps == null || steps.isEmpty()) {
            return null;
        }

        String bestMatch = null;
        int bestLength = -1;
        for (PipelineYamlStep step : steps) {
            String baseToken = toClassToken(step.name());
            if (baseToken.isBlank()) {
                continue;
            }
            if (stepClassName.contains(baseToken) && baseToken.length() > bestLength) {
                bestMatch = useInputType ? step.inputType() : step.outputType();
                bestLength = baseToken.length();
            }
        }
        if (bestMatch == null && LOG.isDebugEnabled()) {
            LOG.debugf("No type mapping found for step %s", stepClassName);
        }
        return bestMatch;
    }

    private static String resolveTypeFromClass(String stepClassName, ClassLoader classLoader, boolean useInputType) {
        try {
            ClassLoader loader = classLoader != null ? classLoader : Thread.currentThread().getContextClassLoader();
            if (loader == null) {
                return null;
            }
            Class<?> stepClass = Class.forName(stepClassName, false, loader);
            for (java.lang.reflect.Type type : stepClass.getGenericInterfaces()) {
                if (!(type instanceof java.lang.reflect.ParameterizedType parameterizedType)) {
                    continue;
                }
                java.lang.reflect.Type raw = parameterizedType.getRawType();
                if (!(raw instanceof Class<?> rawClass)) {
                    continue;
                }
                if (!isPipelineStepInterface(rawClass)) {
                    continue;
                }
                java.lang.reflect.Type[] args = parameterizedType.getActualTypeArguments();
                if (args.length < 2) {
                    continue;
                }
                return simpleTypeName(useInputType ? args[0] : args[1]);
            }
        } catch (ClassNotFoundException ignored) {
            return null;
        }
        return null;
    }

    private static boolean isPipelineStepInterface(Class<?> rawClass) {
        String name = rawClass.getName();
        return name.startsWith("org.pipelineframework.step.")
            && (name.endsWith("StepOneToOne")
            || name.endsWith("StepOneToMany")
            || name.endsWith("StepManyToOne")
            || name.endsWith("StepManyToMany")
            || name.endsWith("StepOneToOneBlocking")
            || name.endsWith("StepOneToManyBlocking")
            || name.endsWith("StepManyToOneBlocking")
            || name.endsWith("StepManyToManyBlocking")
            || name.endsWith("StepOneToOneCompletableFuture"));
    }

    private static String simpleTypeName(java.lang.reflect.Type type) {
        String name = type.getTypeName();
        int lastDot = name.lastIndexOf('.');
        String simple = lastDot == -1 ? name : name.substring(lastDot + 1);
        int inner = simple.lastIndexOf('$');
        return inner == -1 ? simple : simple.substring(inner + 1);
    }

    private static String resolveBasePackage(List<String> baseOrder, String configuredBasePackage) {
        if (configuredBasePackage != null && !configuredBasePackage.isBlank()) {
            return configuredBasePackage;
        }
        for (String stepClassName : baseOrder) {
            if (stepClassName == null || stepClassName.isBlank()) {
                continue;
            }
            int marker = stepClassName.indexOf(".service.pipeline.");
            if (marker > 0) {
                return stepClassName.substring(0, marker);
            }
        }
        return null;
    }

    private static String toClassToken(String name) {
        if (name == null) {
            return "";
        }
        return name.replaceAll("[^A-Za-z0-9]", "");
    }

    private static String normalizeStepToken(String name) {
        if (name == null) {
            return "";
        }
        String simple = name;
        int lastDot = simple.lastIndexOf('.');
        if (lastDot != -1) {
            simple = simple.substring(lastDot + 1);
        }
        simple = simple.replaceAll("(Service|GrpcClientStep|RestClientStep)(_Subclass)?$", "");
        return toClassToken(simple);
    }

    private static String buildSideEffectClientStep(
        String stepClassName,
        String basePackage,
        String aspectName,
        String outputType,
        String clientSuffix
    ) {
        String normalizedType = stripDtoSuffix(outputType);
        String className = toPascalCase(aspectName) + normalizedType + "SideEffect" + clientSuffix;
        if (LOG.isDebugEnabled()) {
            LOG.debugf("Resolved side-effect client step: aspect=%s type=%s normalized=%s class=%s",
                aspectName, outputType, normalizedType, className);
        }
        String packageName = null;
        if (stepClassName != null) {
            int lastDot = stepClassName.lastIndexOf('.');
            if (lastDot > 0) {
                packageName = stepClassName.substring(0, lastDot);
            }
        }
        if (packageName == null || packageName.isBlank()) {
            if (basePackage == null || basePackage.isBlank()) {
                return className;
            }
            packageName = basePackage + ".service.pipeline";
        }
        return packageName + "." + className;
    }

    private static String stripDtoSuffix(String typeName) {
        if (typeName == null || typeName.isBlank()) {
            return "";
        }
        return typeName.endsWith("Dto") ? typeName.substring(0, typeName.length() - 3) : typeName;
    }

    private static String resolveClientStepSuffix(PipelineYamlConfig config) {
        if (config == null || config.transport() == null) {
            return "GrpcClientStep";
        }
        return "REST".equalsIgnoreCase(config.transport()) ? "RestClientStep" : "GrpcClientStep";
    }

    private static String capitalize(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }
        return input.substring(0, 1).toUpperCase(Locale.ROOT) + input.substring(1);
    }

    private static String toPascalCase(String input) {
        if (input == null || input.isBlank()) {
            return input;
        }
        StringBuilder builder = new StringBuilder();
        String[] parts = input.split("[^A-Za-z0-9]+");
        for (String part : parts) {
            if (part.isBlank()) {
                continue;
            }
            builder.append(capitalize(part.toLowerCase(Locale.ROOT)));
        }
        return builder.toString();
    }
}
