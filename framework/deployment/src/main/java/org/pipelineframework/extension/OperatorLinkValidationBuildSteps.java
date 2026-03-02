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

    @BuildStep
    void validateOperatorLinks(
            List<OperatorBuildItem> steps,
            MapperRegistryBuildItem mapperRegistry,
            CombinedIndexBuildItem combinedIndex) {
        validateOperatorLinks(steps, mapperRegistry, combinedIndex.getIndex());
    }

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

    private String mapperNames(List<ClassInfo> matches) {
        List<String> names = new ArrayList<>(matches.size());
        for (ClassInfo match : matches) {
            names.add(match.name().toString());
        }
        return String.join(", ", names);
    }

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

    private Type toReferenceType(Type type) {
        if (type == null) {
            return null;
        }
        if (type.kind() == Type.Kind.PRIMITIVE) {
            return PrimitiveType.box(type.asPrimitiveType());
        }
        return type;
    }

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
