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

    /**
     * Resolve operator references declared in the pipeline configuration, validate and normalize
     * their class/method metadata against the build-time Jandex index, and emit corresponding
     * OperatorBuildItem instances.
     *
     * @param combinedIndex   the combined Jandex index used to resolve classes and types
     * @param pipelineConfig  the pipeline configuration containing step operator references
     * @param operatorProducer producer used to emit resulting OperatorBuildItem instances
     * @throws DeploymentException if any validation or resolution fails, including null inputs,
     *                             a null steps list, malformed operator reference strings,
     *                             unresolved operator classes or methods, ambiguous method overloads,
     *                             or invalid method signatures (more than one parameter, non-public,
     *                             or abstract methods)
     */
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

    /**
     * Parses the operator reference from the step configuration in the form "fully.qualified.Class::method".
     *
     * @param step the step configuration containing the operator string
     * @return an OperatorRef containing the original reference string, the operator class name as a DotName, and the method name
     */
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

    /**
     * Resolve the operator's class from the Jandex index.
     *
     * @param combinedIndex the build-time combined Jandex index to search
     * @param step the pipeline step configuration used to provide contextual information for errors
     * @param ref the parsed operator reference containing the target class name
     * @return the ClassInfo for the resolved operator class
     * @throws DeploymentException if the class named in `ref` is not found in the index
     */
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

    /**
     * Locate the single non-constructor, non-static-initializer, non-synthetic instance method with the specified name
     * on the given class or its superclasses (stops at the first superclass that declares matching methods).
     *
     * @param combinedIndex the build-time Jandex index provider used to traverse superclasses
     * @param step the pipeline step configuration that references the operator (used for error messages)
     * @param ref the parsed operator reference containing the target method name
     * @param operatorClass the class to begin the search from
     * @return the resolved MethodInfo for the operator method
     * @throws DeploymentException if no matching method is found or if multiple overloads are found (ambiguity)
     */
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

    /**
     * Ensure the resolved operator method is public, not abstract, and accepts at most one parameter.
     *
     * @param step   the pipeline step configuration used to produce contextual error messages
     * @param ref    the parsed operator reference used to produce contextual error messages
     * @param method the resolved method to validate
     * @throws DeploymentException if the method declares more than one parameter, is not public, or is abstract
     */
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

    /**
     * Normalize an operator method's raw return type into the framework's canonical representation.
     *
     * <p>The method maps reactive and collection-like return types to a ParameterizedType using the
     * internal Uni/Multi wrappers:
     * - `Uni<T>` remains `Uni<T>` (element type extracted)
     * - `Multi<T>` remains `Multi<T>` (element type extracted)
     * - `Stream<T>` is treated as `Multi<T>`
     * - `CompletionStage<T>` is treated as `Uni<T>`
     * - Collection-like types with a single type argument are treated as `Multi<T>`
     * - Any other return type is wrapped as `Uni<T>` with the original type as the element
     * </p>
     *
     * @param combinedIndex the build-time Jandex index used to detect collection-like types
     * @param step          the pipeline step configuration (used for error context)
     * @param ref           the parsed operator reference (used for error context)
     * @param rawReturnType the original return type reported by Jandex for the operator method
     * @return a Jandex Type representing the normalized return type (a parameterized `Uni<T>` or `Multi<T>` with the resolved element type)
     */
    private Type normalizeReturnType(
            CombinedIndexBuildItem combinedIndex,
            PipelineConfigBuildItem.StepConfig step,
            OperatorRef ref,
            Type rawReturnType) {
        DotName rawName = rawTypeName(rawReturnType);

        if (isUni(rawName)) {
            Type element = extractSingleTypeArgument(rawReturnType, step, ref, "Uni");
            return ParameterizedType.create(UNI, new Type[]{toReferenceTypeOrFail(element, step, ref)}, null);
        }
        if (isMulti(rawName)) {
            Type element = extractSingleTypeArgument(rawReturnType, step, ref, "Multi");
            return ParameterizedType.create(MULTI, new Type[]{toReferenceTypeOrFail(element, step, ref)}, null);
        }
        if (isStream(rawName)) {
            Type element = extractSingleTypeArgument(rawReturnType, step, ref, "Stream");
            return ParameterizedType.create(MULTI, new Type[]{toReferenceTypeOrFail(element, step, ref)}, null);
        }
        if (isCompletionStage(rawName)) {
            Type element = extractSingleTypeArgument(rawReturnType, step, ref, "CompletionStage");
            return ParameterizedType.create(UNI, new Type[]{toReferenceTypeOrFail(element, step, ref)}, null);
        }
        if (isCollectionLike(combinedIndex, rawName)) {
            Type element = extractSingleTypeArgument(rawReturnType, step, ref, "Collection");
            return ParameterizedType.create(MULTI, new Type[]{toReferenceTypeOrFail(element, step, ref)}, null);
        }

        // Any non-reactive/non-collection return is normalized to Uni<T>.
        return ParameterizedType.create(UNI, new Type[]{toReferenceTypeOrFail(rawReturnType, step, ref)}, null);
    }

    /**
     * Obtain the DotName for a Jandex Type, treating `null` and `void` as absent.
     *
     * @param type the Jandex Type to inspect
     * @return the type's DotName, or `null` if `type` is `null` or represents `void`
     */
    private DotName rawTypeName(Type type) {
        if (type == null || type.kind() == Type.Kind.VOID) {
            return null;
        }
        return type.name();
    }

    /**
     * Extracts the single generic type argument from a parameterized type.
     *
     * @param type the parameterized type to inspect
     * @param step the pipeline step whose operator is being validated (used in error messages)
     * @param ref  the parsed operator reference (used in error messages)
     * @param logicalTypeName a human-readable name for the expected generic type (used in error messages)
     * @return the sole type argument declared on the provided parameterized type
     * @throws DeploymentException if the provided type is not parameterized or declares a number of
     *                             generic arguments other than one
     */
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

    /**
     * Determines whether the given type name represents `Uni`.
     *
     * @param rawName the type name to check
     * @return `true` if the name equals the `Uni` type, `false` otherwise
     */
    private boolean isUni(DotName rawName) {
        return UNI.equals(rawName);
    }

    /**
     * Determines whether the provided raw type name corresponds to the Multi reactive type.
     *
     * @param rawName the raw type name to check
     * @return `true` if `rawName` equals the configured `MULTI` type, `false` otherwise
     */
    private boolean isMulti(DotName rawName) {
        return MULTI.equals(rawName);
    }

    /**
     * Checks whether a raw type name represents a `Stream`.
     *
     * @param rawName the raw type name to check
     * @return `true` if `rawName` denotes a `Stream`, `false` otherwise
     */
    private boolean isStream(DotName rawName) {
        return STREAM.equals(rawName);
    }

    /**
     * Determine if the given raw type name represents `CompletionStage`.
     *
     * @param rawName the raw type name to check
     * @return `true` if `rawName` equals `CompletionStage`, `false` otherwise
     */
    private boolean isCompletionStage(DotName rawName) {
        return COMPLETION_STAGE.equals(rawName);
    }

    /**
     * Determine whether the given type name represents `java.util.Collection` or is assignable to it.
     *
     * @param combinedIndex the build-time combined Jandex index used to resolve classes
     * @param rawName the dot-name of the type to check; may be null
     * @return `true` if `rawName` equals `java.util.Collection` or resolves to a class assignable to it; `false` otherwise (including when `rawName` is null or cannot be resolved)
     */
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

    /**
     * Determines whether the given class (or any of its superclasses or interfaces) is assignable to the specified target type.
     *
     * @param classInfo the class to check; may be null
     * @param target the type name to test assignability against
     * @param combinedIndex the index used to resolve superclasses and interfaces
     * @param visited a set of already-visited type names used to avoid infinite recursion
     * @return `true` if `classInfo` is the same as, implements, or extends `target`; `false` otherwise
     */
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

    /**
     * Normalize a Jandex Type to a reference-compatible form.
     *
     * @param type the Jandex Type to normalize
     * @return a Type suitable for reference usage: `java.lang.Void` when input is `void`, a boxed primitive
     *         when input is a primitive, the same parameterized type when input is parameterized, or the
     *         original type otherwise
     * @throws IllegalArgumentException if the type is a wildcard, type variable, or unresolved type variable
     */
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

    private Type toReferenceTypeOrFail(Type type, PipelineConfigBuildItem.StepConfig step, OperatorRef ref) {
        try {
            return toReferenceType(type);
        } catch (IllegalArgumentException e) {
            throw new DeploymentException(
                    "Failed to normalize return type for step '" + step.name()
                            + "' (operator '" + ref.raw() + "'): " + e.getMessage(),
                    e);
        }
    }

    private record OperatorRef(String raw, DotName className, String methodName) {
    }
}
