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
import jakarta.enterprise.inject.spi.DeploymentException;
import org.jboss.logging.Logger;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.ParameterizedType;
import org.jboss.jandex.PrimitiveType;
import org.jboss.jandex.Type;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Resolves YAML operator references to Jandex class/method metadata at build time.
 */
public final class OperatorResolutionBuildSteps {

    private static final Logger LOG = Logger.getLogger(OperatorResolutionBuildSteps.class);
    private static final DotName UNI = DotName.createSimple("io.smallrye.mutiny.Uni");
    private static final DotName MULTI = DotName.createSimple("io.smallrye.mutiny.Multi");
    private static final DotName COLLECTION = DotName.createSimple("java.util.Collection");
    private static final DotName STREAM = DotName.createSimple("java.util.stream.Stream");
    private static final DotName COMPLETION_STAGE = DotName.createSimple("java.util.concurrent.CompletionStage");
    private static final DotName JAVA_LANG_OBJECT = DotName.createSimple("java.lang.Object");
    private static final Type JAVA_LANG_VOID = Type.create(DotName.createSimple("java.lang.Void"), Type.Kind.CLASS);

    @BuildStep
    void resolveOperators(
            CombinedIndexBuildItem combinedIndex,
            PipelineConfigBuildItem pipelineConfig,
            BuildProducer<OperatorBuildItem> operatorProducer) {

        Objects.requireNonNull(combinedIndex, "combinedIndex must not be null");
        Objects.requireNonNull(pipelineConfig, "pipelineConfig must not be null");
        Objects.requireNonNull(operatorProducer, "operatorProducer must not be null");

        if (pipelineConfig.steps() == null) {
            throw new DeploymentException("Pipeline config steps must not be null");
        }

        for (PipelineConfigBuildItem.StepConfig step : pipelineConfig.steps()) {
            if (step == null) {
                throw new DeploymentException("Pipeline step config contains a null step entry");
            }

            OperatorRef ref = parseOperator(step);
            ClassInfo operatorClass = resolveOperatorClass(combinedIndex, step, ref);
            MethodInfo method = resolveOperatorMethod(combinedIndex, step, ref, operatorClass);
            validateMethodSignature(step, ref, method);

            Type inputType = method.parametersCount() == 0 ? JAVA_LANG_VOID : method.parameterType(0);
            Type rawReturnType = method.returnType();
            OperatorCategory category = classify(rawReturnType);
            Type normalizedReturnType = normalizeReturnType(combinedIndex, step, ref, rawReturnType);

            operatorProducer.produce(new OperatorBuildItem(
                    step,
                    operatorClass,
                    method,
                    inputType,
                    normalizedReturnType,
                    category));
        }
    }

    private OperatorRef parseOperator(PipelineConfigBuildItem.StepConfig step) {
        String value = step.operator();
        if (value == null || value.isBlank()) {
            throw new DeploymentException("Step '" + step.name() + "' has empty operator reference; expected 'fully.qualified.Class::method'");
        }

        int separator = value.indexOf("::");
        if (separator <= 0 || separator == value.length() - 2 || value.indexOf("::", separator + 2) != -1) {
            throw new DeploymentException("Step '" + step.name() + "' has invalid operator reference '" + value
                    + "'; expected exactly one '::' in 'fully.qualified.Class::method' format");
        }

        String className = value.substring(0, separator).trim();
        String methodName = value.substring(separator + 2).trim();
        if (className.isEmpty() || methodName.isEmpty()) {
            throw new DeploymentException("Step '" + step.name() + "' has invalid operator reference '" + value
                    + "'; class and method must both be non-blank");
        }

        return new OperatorRef(value, DotName.createSimple(className), methodName);
    }

    private ClassInfo resolveOperatorClass(
            CombinedIndexBuildItem combinedIndex,
            PipelineConfigBuildItem.StepConfig step,
            OperatorRef ref) {
        ClassInfo classInfo = combinedIndex.getIndex().getClassByName(ref.className());
        if (classInfo == null) {
            throw new DeploymentException("Step '" + step.name() + "' operator '" + ref.raw()
                    + "' cannot be resolved: class '" + ref.className() + "' not found in Jandex index");
        }
        return classInfo;
    }

    private MethodInfo resolveOperatorMethod(
            CombinedIndexBuildItem combinedIndex,
            PipelineConfigBuildItem.StepConfig step,
            OperatorRef ref,
            ClassInfo operatorClass) {
        List<MethodInfo> matches = new ArrayList<>();
        ClassInfo cursor = operatorClass;
        while (cursor != null) {
            List<MethodInfo> currentMatches = new ArrayList<>();
            for (MethodInfo method : cursor.methods()) {
                if (ref.methodName().equals(method.name())
                        && !method.isConstructor()
                        && !method.isStaticInitializer()
                        && !method.isSynthetic()) {
                    currentMatches.add(method);
                }
            }
            if (!currentMatches.isEmpty()) {
                matches.addAll(currentMatches);
                break;
            }
            Type superType = cursor.superClassType();
            if (superType == null || superType.name() == null || JAVA_LANG_OBJECT.equals(superType.name())) {
                break;
            }
            cursor = combinedIndex.getIndex().getClassByName(superType.name());
        }

        if (matches.isEmpty()) {
            throw new DeploymentException("Step '" + step.name() + "' operator '" + ref.raw()
                    + "' cannot be resolved: no method named '" + ref.methodName()
                    + "' found on class '" + operatorClass.name() + "'");
        }
        if (matches.size() > 1) {
            throw new DeploymentException("Step '" + step.name() + "' operator '" + ref.raw()
                    + "' is ambiguous: found " + matches.size()
                    + " methods named '" + ref.methodName() + "' on class '" + operatorClass.name()
                    + "'. Overloads are not supported.");
        }
        return matches.get(0);
    }

    private void validateMethodSignature(PipelineConfigBuildItem.StepConfig step, OperatorRef ref, MethodInfo method) {
        if (method.parametersCount() > 1) {
            throw new DeploymentException("Step '" + step.name() + "' operator '" + ref.raw()
                    + "' is invalid: method '" + method.name() + "' has " + method.parametersCount()
                    + " parameters; at most one parameter is allowed");
        }
        if (!Modifier.isPublic(method.flags())) {
            throw new DeploymentException("Step '" + step.name() + "' operator '" + ref.raw()
                    + "' is invalid: method '" + method.name() + "' must be public");
        }
        if (Modifier.isAbstract(method.flags())) {
            throw new DeploymentException("Step '" + step.name() + "' operator '" + ref.raw()
                    + "' is invalid: method '" + method.name() + "' must not be abstract");
        }
    }

    /**
     * Classifies return types based on their raw type.
     * <p>
     * When {@code rawTypeName(type)} returns {@code null} (void return), classification is intentionally
     * {@link OperatorCategory#NON_REACTIVE}; later, {@code normalizeReturnType(...)} maps void to {@code Uni<Void>}.
     * This means {@link OperatorBuildItem} can legitimately contain NON_REACTIVE with normalized Uni return metadata.
     */
    private OperatorCategory classify(Type rawReturnType) {
        DotName rawName = rawTypeName(rawReturnType);
        return isUni(rawName) || isMulti(rawName) || isStream(rawName) || isCompletionStage(rawName)
                ? OperatorCategory.REACTIVE
                : OperatorCategory.NON_REACTIVE;
    }

    private Type normalizeReturnType(
            CombinedIndexBuildItem combinedIndex,
            PipelineConfigBuildItem.StepConfig step,
            OperatorRef ref,
            Type rawReturnType) {
        DotName rawName = rawTypeName(rawReturnType);

        if (isUni(rawName)) {
            Type element = extractSingleTypeArgument(rawReturnType, step, ref, "Uni");
            return ParameterizedType.create(UNI, new Type[]{toReferenceType(element)}, null);
        }
        if (isMulti(rawName)) {
            Type element = extractSingleTypeArgument(rawReturnType, step, ref, "Multi");
            return ParameterizedType.create(MULTI, new Type[]{toReferenceType(element)}, null);
        }
        if (isStream(rawName)) {
            Type element = extractSingleTypeArgument(rawReturnType, step, ref, "Stream");
            return ParameterizedType.create(MULTI, new Type[]{toReferenceType(element)}, null);
        }
        if (isCompletionStage(rawName)) {
            Type element = extractSingleTypeArgument(rawReturnType, step, ref, "CompletionStage");
            return ParameterizedType.create(UNI, new Type[]{toReferenceType(element)}, null);
        }
        if (isCollectionLike(combinedIndex, rawName)) {
            Type element = extractSingleTypeArgument(rawReturnType, step, ref, "Collection");
            return ParameterizedType.create(MULTI, new Type[]{toReferenceType(element)}, null);
        }

        // Any non-reactive/non-collection return is normalized to Uni<T>.
        return ParameterizedType.create(UNI, new Type[]{toReferenceType(rawReturnType)}, null);
    }

    private DotName rawTypeName(Type type) {
        if (type == null || type.kind() == Type.Kind.VOID) {
            return null;
        }
        return type.name();
    }

    private Type extractSingleTypeArgument(
            Type type,
            PipelineConfigBuildItem.StepConfig step,
            OperatorRef ref,
            String logicalTypeName) {
        if (type.kind() != Type.Kind.PARAMETERIZED_TYPE) {
            throw new DeploymentException("Step '" + step.name() + "' operator '" + ref.raw()
                    + "' is invalid: " + logicalTypeName + " return type is raw/not parameterized");
        }
        List<Type> args = type.asParameterizedType().arguments();
        if (args.size() != 1) {
            throw new DeploymentException("Step '" + step.name() + "' operator '" + ref.raw()
                    + "' is invalid: " + logicalTypeName + " return type declares " + args.size()
                    + " generic arguments; expected exactly one");
        }
        return args.get(0);
    }

    private boolean isUni(DotName rawName) {
        return UNI.equals(rawName);
    }

    private boolean isMulti(DotName rawName) {
        return MULTI.equals(rawName);
    }

    private boolean isStream(DotName rawName) {
        return STREAM.equals(rawName);
    }

    private boolean isCompletionStage(DotName rawName) {
        return COMPLETION_STAGE.equals(rawName);
    }

    private boolean isCollectionLike(CombinedIndexBuildItem combinedIndex, DotName rawName) {
        if (rawName == null) {
            return false;
        }
        if (COLLECTION.equals(rawName)) {
            return true;
        }
        ClassInfo classInfo = combinedIndex.getIndex().getClassByName(rawName);
        if (classInfo == null) {
            LOG.warnf("isCollectionLike could not resolve type '%s' in CombinedIndexBuildItem; treating as non-collection", rawName);
            return false;
        }
        return isAssignableTo(classInfo, COLLECTION, combinedIndex, new HashSet<>());
    }

    private boolean isAssignableTo(
            ClassInfo classInfo,
            DotName target,
            CombinedIndexBuildItem combinedIndex,
            Set<DotName> visited) {
        if (classInfo == null) {
            return false;
        }
        DotName name = classInfo.name();
        if (name == null || !visited.add(name)) {
            return false;
        }
        if (target.equals(classInfo.name())) {
            return true;
        }

        for (Type itf : classInfo.interfaceTypes()) {
            DotName itfName = itf.name();
            if (target.equals(itfName)) {
                return true;
            }
            ClassInfo itfClass = itfName == null ? null : combinedIndex.getIndex().getClassByName(itfName);
            if (isAssignableTo(itfClass, target, combinedIndex, visited)) {
                return true;
            }
        }

        Type superType = classInfo.superClassType();
        if (superType == null) {
            return false;
        }
        DotName superName = superType.name();
        if (target.equals(superName)) {
            return true;
        }
        ClassInfo superClass = superName == null ? null : combinedIndex.getIndex().getClassByName(superName);
        return isAssignableTo(superClass, target, combinedIndex, visited);
    }

    private Type toReferenceType(Type type) {
        if (type == null || type.kind() == Type.Kind.VOID) {
            return JAVA_LANG_VOID;
        }
        if (type.kind() == Type.Kind.PRIMITIVE) {
            return PrimitiveType.box(type.asPrimitiveType());
        }
        if (type.kind() == Type.Kind.PARAMETERIZED_TYPE) {
            return type.asParameterizedType();
        }
        if (type.kind() == Type.Kind.WILDCARD_TYPE
                || type.kind() == Type.Kind.TYPE_VARIABLE
                || type.kind() == Type.Kind.UNRESOLVED_TYPE_VARIABLE) {
            throw new IllegalArgumentException(
                    "Unsupported type kind for normalization: " + type.kind() + " for type " + type);
        }
        return type;
    }

    private record OperatorRef(String raw, DotName className, String methodName) {
    }
}
