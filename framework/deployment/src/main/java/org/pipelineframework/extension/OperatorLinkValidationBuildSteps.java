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

import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import jakarta.enterprise.inject.spi.DeploymentException;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;
import org.jboss.jandex.PrimitiveType;
import org.jboss.jandex.Type;
import org.pipelineframework.extension.MapperInferenceEngine.MapperPairKey;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Validates adjacent operator links at build time.
 */
public final class OperatorLinkValidationBuildSteps {

    private static final DotName UNI = DotName.createSimple("io.smallrye.mutiny.Uni");
    private static final DotName MULTI = DotName.createSimple("io.smallrye.mutiny.Multi");
    private static final DotName JAVA_LANG_OBJECT = DotName.createSimple("java.lang.Object");

    /**
     * Triggers build-time validation of adjacent operator links, ensuring each step's produced type is compatible
     * with the next step's expected type using the provided mapper registry and combined index.
     *
     * @param steps          the ordered list of operator build items to validate
     * @param mapperRegistry the mapper registry used to resolve domain-to-target mappings when types differ
     * @param combinedIndex  the combined Jandex index of application classes used for type assignability checks
     */
    @BuildStep
    void validateOperatorLinks(
            List<OperatorBuildItem> steps,
            MapperRegistryBuildItem mapperRegistry,
            CombinedIndexBuildItem combinedIndex) {
        validateOperatorLinks(steps, mapperRegistry, combinedIndex.getIndex());
    }

    /**
     * Validate compatibility between adjacent operator build steps' outputs and the next step's inputs.
     *
     * Performs cardinality checks (rejects Multi feeding Uni), ensures the produced type is assignable to the
     * next step's expected type, and when not assignable consults the mapper registry for mappers matching the
     * produced domain type. Throws a deployment exception describing the mismatch if validation fails.
     *
     * @param steps the ordered list of operator build steps to validate; adjacent pairs are compared
     * @param mapperRegistry registry used to discover mappers for produced domain types
     * @param index the Jandex index view used to determine type assignability
     * @throws NullPointerException if {@code steps}, {@code mapperRegistry}, {@code index}, or any required step is null
     * @throws DeploymentException if a cardinality mismatch, an unassignable type, no mapper, or multiple mappers are found
     */
    void validateOperatorLinks(
            List<OperatorBuildItem> steps,
            MapperRegistryBuildItem mapperRegistry,
            IndexView index) {
        Objects.requireNonNull(steps, "steps must not be null");
        Objects.requireNonNull(mapperRegistry, "mapperRegistry must not be null");
        Objects.requireNonNull(index, "index must not be null");

        for (int i = 0; i < steps.size() - 1; i++) {
            OperatorBuildItem current = Objects.requireNonNull(steps.get(i), "steps[" + i + "] must not be null");
            OperatorBuildItem next = Objects.requireNonNull(steps.get(i + 1), "steps[" + (i + 1) + "] must not be null");

            ResolvedType produced = unwrapReactive(current.normalizedReturnType());
            ResolvedType expected = unwrapReactive(next.inputType());

            if (produced.cardinality() == Cardinality.MULTI && expected.cardinality() == Cardinality.UNI) {
                throw mismatch(current, produced.type(), next, expected.type(),
                        "cardinality mismatch: Multi<T> cannot feed Uni<T>");
            }

            if (isAssignable(produced.type(), expected.type(), index)) {
                continue;
            }

            List<ClassInfo> matches = findMappersByDomain(mapperRegistry, typeName(produced.type()));
            if (matches.isEmpty()) {
                throw mismatch(current, produced.type(), next, expected.type(),
                        "no mapper found for produced domain type " + produced.type());
            }
            if (matches.size() > 1) {
                throw mismatch(current, produced.type(), next, expected.type(),
                        "multiple mappers found for produced domain type " + produced.type() + ": " + mapperNames(matches));
            }
        }
    }

    /**
     * Finds mappers registered for the given domain type.
     *
     * @param domainType the domain type's DotName to match; if null, the method returns an empty list
     * @return a list of ClassInfo for mappers whose MapperPairKey domainType equals the provided domainType; empty if none found
     */
    private List<ClassInfo> findMappersByDomain(MapperRegistryBuildItem mapperRegistry, DotName domainType) {
        List<ClassInfo> matches = new ArrayList<>();
        if (domainType == null) {
            return matches;
        }
        for (var entry : mapperRegistry.getPairToMapperMap().entrySet()) {
            MapperPairKey key = entry.getKey();
            if (domainType.equals(key.domainType())) {
                matches.add(entry.getValue());
            }
        }
        return matches;
    }

    /**
     * Produces a comma-separated list of fully-qualified class names from the provided matches.
     *
     * @param matches list of ClassInfo objects whose names will be used; may be empty
     * @return a string containing the fully-qualified names joined by ", ", or an empty string if {@code matches} is empty
     */
    private String mapperNames(List<ClassInfo> matches) {
        List<String> names = new ArrayList<>(matches.size());
        for (ClassInfo match : matches) {
            names.add(match.name().toString());
        }
        return String.join(", ", names);
    }

    /**
     * Constructs a DeploymentException describing a type mismatch between two adjacent operator steps.
     *
     * @param current the operator that produces the value
     * @param producedType the actual type produced by the current operator
     * @param next the operator that consumes the value
     * @param expectedType the type expected by the next operator
     * @param detail additional context to include in the error message
     * @return a DeploymentException with a formatted message identifying the producing step, the produced type,
     *         the consuming step, the expected type, and the provided detail
     */
    private DeploymentException mismatch(
            OperatorBuildItem current,
            Type producedType,
            OperatorBuildItem next,
            Type expectedType,
            String detail) {
        return new DeploymentException(String.format(
                "Step '%s' produces %s but step '%s' expects %s (%s)",
                current.step().name(),
                producedType,
                next.step().name(),
                expectedType,
                detail));
    }

    /**
     * Unwraps a Mutiny reactive type (`Uni` or `Multi`) to its inner element type and indicates whether it represents
     * a single value or multiple values.
     *
     * @param type the type to inspect; may be parameterized (e.g., `Uni<T>`/`Multi<T>`) or a raw type
     * @return a {@code ResolvedType} whose {@code type} is the inner type when {@code type} is a parameterized `Uni` or
     *         `Multi` with exactly one type argument, otherwise the original {@code type}; the {@code Cardinality} is
     *         `MULTI` when the raw type is `Multi`, `UNI` otherwise
     */
    private ResolvedType unwrapReactive(Type type) {
        DotName raw = typeName(type);
        if (type != null && type.kind() == Type.Kind.PARAMETERIZED_TYPE && (UNI.equals(raw) || MULTI.equals(raw))) {
            List<Type> arguments = type.asParameterizedType().arguments();
            if (arguments.size() == 1) {
                return new ResolvedType(UNI.equals(raw) ? Cardinality.UNI : Cardinality.MULTI, arguments.get(0));
            }
        }
        return new ResolvedType(MULTI.equals(raw) ? Cardinality.MULTI : Cardinality.UNI, type);
    }

    /**
     * Determines whether a value of `fromType` can be assigned to a variable of `toType`,
     * taking into account primitive boxing and the type hierarchy available via the index.
     *
     * @param fromType the source type to assign from; may be a primitive or reference type
     * @param toType the target type to assign to; may be a primitive or reference type
     * @param index the index used to resolve class and interface hierarchies
     * @return `true` if a value of `fromType` is assignable to `toType`, `false` otherwise
     */
    private boolean isAssignable(Type fromType, Type toType, IndexView index) {
        if (fromType == null || toType == null) {
            return false;
        }
        if (fromType.equals(toType) || fromType.toString().equals(toType.toString())) {
            return true;
        }

        Type fromRef = toReferenceType(fromType);
        Type toRef = toReferenceType(toType);
        DotName fromName = typeName(fromRef);
        DotName toName = typeName(toRef);

        if (fromName == null || toName == null) {
            return false;
        }
        if (fromName.equals(toName)) {
            return true;
        }

        ClassInfo fromClass = index.getClassByName(fromName);
        return isAssignableTo(fromClass, toName, index, new HashSet<>());
    }

    /**
     * Recursively determines whether the provided class or any of its superclasses or implemented interfaces matches the given target type name.
     *
     * @param classInfo the class to inspect; may be null
     * @param target the DotName of the target type to match
     * @param index the index view used to resolve referenced classes by name
     * @param visited a set of already visited type names to avoid infinite recursion caused by cycles
     * @return `true` if `classInfo` or any traversed superclass/interface has a name equal to `target`, `false` otherwise
     */
    private boolean isAssignableTo(ClassInfo classInfo, DotName target, IndexView index, Set<DotName> visited) {
        if (classInfo == null || classInfo.name() == null || !visited.add(classInfo.name())) {
            return false;
        }
        if (target.equals(classInfo.name())) {
            return true;
        }

        for (Type interfaceType : classInfo.interfaceTypes()) {
            DotName interfaceName = interfaceType.name();
            if (target.equals(interfaceName)) {
                return true;
            }
            ClassInfo interfaceClass = interfaceName == null ? null : index.getClassByName(interfaceName);
            if (isAssignableTo(interfaceClass, target, index, visited)) {
                return true;
            }
        }

        Type superType = classInfo.superClassType();
        if (superType == null || superType.name() == null || JAVA_LANG_OBJECT.equals(superType.name())) {
            return false;
        }
        if (target.equals(superType.name())) {
            return true;
        }
        return isAssignableTo(index.getClassByName(superType.name()), target, index, visited);
    }

    /**
     * Convert a primitive Type to its corresponding boxed reference Type; leaves reference types unchanged.
     *
     * @param type the type to convert; may be null
     * @return the boxed reference Type if the input is a primitive, the original type if it's already a reference, or `null` if the input is null
     */
    private Type toReferenceType(Type type) {
        if (type == null) {
            return null;
        }
        if (type.kind() == Type.Kind.PRIMITIVE) {
            return PrimitiveType.box(type.asPrimitiveType());
        }
        return type;
    }

    /**
     * Obtain the DotName of the provided Type, or null when the input is null.
     *
     * @param type the type whose DotName is required; may be null
     * @return the type's DotName, or null if {@code type} is null
     */
    private DotName typeName(Type type) {
        return type == null ? null : type.name();
    }

    private enum Cardinality {
        UNI,
        MULTI
    }

    private record ResolvedType(Cardinality cardinality, Type type) {
    }
}
