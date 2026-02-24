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

import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.deployment.GeneratedBeanBuildItem;
import io.quarkus.arc.deployment.GeneratedBeanGizmoAdaptor;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.gizmo.ClassCreator;
import io.quarkus.gizmo.ClassOutput;
import io.quarkus.gizmo.FieldCreator;
import io.quarkus.gizmo.FieldDescriptor;
import io.quarkus.gizmo.MethodCreator;
import io.quarkus.gizmo.MethodDescriptor;
import io.quarkus.gizmo.ResultHandle;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.groups.MultiCreate;
import io.smallrye.mutiny.groups.UniCreate;
import jakarta.enterprise.inject.spi.DeploymentException;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.Type;
import org.pipelineframework.service.ReactiveService;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

/**
 * Generates build-time operator invoker CDI beans from {@link OperatorBuildItem}.
 */
public final class OperatorInvokerBuildSteps {

    private static final DotName UNI = DotName.createSimple("io.smallrye.mutiny.Uni");
    private static final DotName MULTI = DotName.createSimple("io.smallrye.mutiny.Multi");
    private static final MethodDescriptor OBJ_CTOR = MethodDescriptor.ofConstructor(Object.class);
    private static final MethodDescriptor UNI_CREATE_FROM = MethodDescriptor.ofMethod(Uni.class, "createFrom", UniCreate.class);
    private static final MethodDescriptor UNI_ITEM = MethodDescriptor.ofMethod(UniCreate.class, "item", Uni.class, Object.class);

    @BuildStep
    void generateOperatorInvokers(
            CombinedIndexBuildItem combinedIndex,
            List<OperatorBuildItem> operators,
            BuildProducer<GeneratedBeanBuildItem> generatedBeans,
            BuildProducer<AdditionalBeanBuildItem> additionalBeans) {

        if (operators == null || operators.isEmpty()) {
            return;
        }

        ClassOutput output = new GeneratedBeanGizmoAdaptor(generatedBeans);
        int suffix = 0;

        for (OperatorBuildItem operator : operators) {
            ValidatedOperator validated = validateOperator(combinedIndex, operator);
            String className = generatedClassName(operator, ++suffix);
            generateInvokerClass(output, className, validated);
            additionalBeans.produce(AdditionalBeanBuildItem.unremovableOf(className));
        }
    }

    private ValidatedOperator validateOperator(CombinedIndexBuildItem combinedIndex, OperatorBuildItem operator) {
        DotName className = operator.operatorClass().name();
        ClassInfo indexedClass = combinedIndex.getIndex().getClassByName(className);
        if (indexedClass == null) {
            throw new DeploymentException("Operator class '" + className + "' for step '" + operator.step().name()
                    + "' is no longer present in Jandex index");
        }

        List<MethodInfo> matches = new ArrayList<>();
        for (MethodInfo candidate : indexedClass.methods()) {
            if (candidate.name().equals(operator.method().name())
                    && !candidate.isConstructor()
                    && !candidate.isStaticInitializer()
                    && !candidate.isBridge()
                    && !candidate.isSynthetic()) {
                matches.add(candidate);
            }
        }
        if (matches.isEmpty()) {
            throw new DeploymentException("Operator method '" + operator.method().name() + "' for step '"
                    + operator.step().name() + "' no longer exists on class '" + className + "'");
        }
        if (matches.size() > 1) {
            throw new DeploymentException("Operator method '" + operator.method().name() + "' for step '"
                    + operator.step().name() + "' is ambiguous on class '" + className + "': found "
                    + matches.size() + " methods with the same name. Overloaded operator methods are not supported.");
        }

        MethodInfo method = matches.get(0);
        if (!sameSignature(method, operator.method())) {
            throw new DeploymentException("Operator method signature changed for step '" + operator.step().name()
                    + "': expected " + operator.method() + " but found " + method);
        }
        if (method.parametersCount() > 1) {
            throw new DeploymentException("Operator method '" + method.name() + "' for step '" + operator.step().name()
                    + "' has " + method.parametersCount() + " parameters; expected at most one");
        }

        ensureGenericReactiveTypeIfNeeded(method.returnType(), operator);

        Type normalized = operator.normalizedReturnType();
        if (normalized.kind() != Type.Kind.PARAMETERIZED_TYPE || normalized.asParameterizedType().arguments().size() != 1) {
            throw new DeploymentException("Normalized return type for step '" + operator.step().name()
                    + "' must be parameterized Uni<T> or Multi<T>, got: " + normalized);
        }

        DotName normalizedRaw = normalized.name();
        if (!UNI.equals(normalizedRaw) && !MULTI.equals(normalizedRaw)) {
            throw new DeploymentException("Normalized return type for step '" + operator.step().name()
                    + "' must be Uni<T> or Multi<T>, got: " + normalized);
        }

        enforcePhaseOneBoundaries(method, operator, normalizedRaw);

        return new ValidatedOperator(indexedClass, method, operator);
    }

    private void enforcePhaseOneBoundaries(MethodInfo method, OperatorBuildItem operator, DotName normalizedRaw) {
        if (method.parametersCount() == 1 && MULTI.equals(method.parameterType(0).name())) {
            throw new DeploymentException("Step '" + operator.step().name()
                    + "' uses streaming input (Multi<T>) which is not supported in Phase 1 operator invokers. "
                    + "Only unary input operators are supported.");
        }
        if (MULTI.equals(normalizedRaw)) {
            throw new DeploymentException("Step '" + operator.step().name()
                    + "' normalizes to Multi<T>, which is not supported in Phase 1 operator invokers. "
                    + "Only unary output (Uni<T>) operators are supported.");
        }
        if (operator.category() == OperatorCategory.REACTIVE && !UNI.equals(method.returnType().name())) {
            throw new DeploymentException("Step '" + operator.step().name()
                    + "' uses reactive return type '" + method.returnType()
                    + "' that is not supported in Phase 1. "
                    + "Only Uni<T> reactive returns are supported.");
        }
    }

    private void ensureGenericReactiveTypeIfNeeded(Type rawReturnType, OperatorBuildItem operator) {
        DotName raw = rawReturnType.name();
        if (!UNI.equals(raw) && !MULTI.equals(raw)) {
            return;
        }
        if (rawReturnType.kind() != Type.Kind.PARAMETERIZED_TYPE
                || rawReturnType.asParameterizedType().arguments().size() != 1) {
            throw new DeploymentException("Step '" + operator.step().name()
                    + "' declares raw " + raw.withoutPackagePrefix()
                    + " return type without a generic parameter on method '" + operator.method().name() + "'");
        }
    }

    private boolean sameSignature(MethodInfo actual, MethodInfo expected) {
        if (!actual.name().equals(expected.name())) {
            return false;
        }
        if (actual.parametersCount() != expected.parametersCount()) {
            return false;
        }
        for (int i = 0; i < actual.parametersCount(); i++) {
            if (!actual.parameterType(i).equals(expected.parameterType(i))) {
                return false;
            }
        }
        return actual.returnType().equals(expected.returnType());
    }

    private void generateInvokerClass(ClassOutput output, String className, ValidatedOperator operator) {
        String serviceInterface = ReactiveService.class.getName();

        try (ClassCreator cc = ClassCreator.builder()
                .classOutput(output)
                .className(className)
                .interfaces(serviceInterface)
                .build()) {
            cc.addAnnotation(Singleton.class);

            FieldDescriptor targetField = null;
            if (!Modifier.isStatic(operator.method().flags())) {
                FieldCreator field = cc.getFieldCreator("target", operator.classInfo().name().toString())
                        .setModifiers(Modifier.PRIVATE);
                field.addAnnotation(Inject.class);
                targetField = field.getFieldDescriptor();
            }

            MethodCreator ctor = cc.getConstructorCreator(new Class<?>[0]).setModifiers(Modifier.PUBLIC);
            ctor.invokeSpecialMethod(OBJ_CTOR, ctor.getThis());
            ctor.returnVoid();

            MethodCreator process = cc.getMethodCreator("process", Uni.class, Object.class)
                    .setModifiers(Modifier.PUBLIC);
            if (operator.buildItem().category() == OperatorCategory.NON_REACTIVE) {
                process.addAnnotation("io.smallrye.common.annotation.Blocking");
            }

            ResultHandle invocationResult = invokeTarget(process, operator, targetField);
            ResultHandle returned = adaptReturn(process, operator, invocationResult);
            process.returnValue(returned);
        }
    }

    private ResultHandle invokeTarget(
            MethodCreator process,
            ValidatedOperator operator,
            FieldDescriptor targetField) {
        MethodDescriptor targetMethod = MethodDescriptor.of(operator.method());
        ResultHandle[] args = buildInvocationArgs(process, operator.method());

        if (Modifier.isStatic(operator.method().flags())) {
            return process.invokeStaticMethod(targetMethod, args);
        }
        ResultHandle target = process.readInstanceField(targetField, process.getThis());
        if (Modifier.isInterface(operator.method().declaringClass().flags())) {
            return process.invokeInterfaceMethod(targetMethod, target, args);
        }
        return process.invokeVirtualMethod(targetMethod, target, args);
    }

    private ResultHandle[] buildInvocationArgs(MethodCreator process, MethodInfo method) {
        if (method.parametersCount() == 0) {
            return new ResultHandle[0];
        }
        ResultHandle rawInput = process.getMethodParam(0);
        ResultHandle converted = castInput(process, rawInput, method.parameterType(0));
        return new ResultHandle[]{converted};
    }

    private ResultHandle castInput(MethodCreator process, ResultHandle input, Type expectedType) {
        if (expectedType.kind() == Type.Kind.PRIMITIVE) {
            ResultHandle boxed = process.checkCast(input, boxedType(expectedType.asPrimitiveType().primitive()).getName());
            return unbox(process, boxed, expectedType.asPrimitiveType().primitive());
        }
        if (expectedType.kind() == Type.Kind.ARRAY) {
            String descriptor = expectedType.descriptor(symbol -> {
                throw new IllegalArgumentException(
                        "Unsupported generic array parameter in operator input type '" + expectedType
                                + "': unresolved symbol '" + symbol + "'");
            });
            return process.checkCast(input, descriptor);
        }
        DotName raw = expectedType.name();
        if (raw == null) {
            throw new DeploymentException("Unsupported operator input type: " + expectedType);
        }
        return process.checkCast(input, raw.toString());
    }

    private ResultHandle adaptReturn(MethodCreator process, ValidatedOperator operator, ResultHandle result) {
        if (operator.buildItem().category() == OperatorCategory.REACTIVE) {
            return process.checkCast(result, Uni.class);
        }

        ResultHandle create = process.invokeStaticMethod(UNI_CREATE_FROM);
        if (operator.method().returnType().kind() == Type.Kind.VOID) {
            return process.invokeVirtualMethod(UNI_ITEM, create, process.loadNull());
        }
        ResultHandle boxed = boxIfPrimitive(process, operator.method().returnType(), result);
        return process.invokeVirtualMethod(UNI_ITEM, create, boxed);
    }

    private ResultHandle boxIfPrimitive(MethodCreator process, Type returnType, ResultHandle value) {
        if (returnType.kind() != Type.Kind.PRIMITIVE) {
            return value;
        }
        return switch (returnType.asPrimitiveType().primitive()) {
            case BOOLEAN -> process.invokeStaticMethod(
                    MethodDescriptor.ofMethod(Boolean.class, "valueOf", Boolean.class, boolean.class), value);
            case BYTE -> process.invokeStaticMethod(
                    MethodDescriptor.ofMethod(Byte.class, "valueOf", Byte.class, byte.class), value);
            case SHORT -> process.invokeStaticMethod(
                    MethodDescriptor.ofMethod(Short.class, "valueOf", Short.class, short.class), value);
            case INT -> process.invokeStaticMethod(
                    MethodDescriptor.ofMethod(Integer.class, "valueOf", Integer.class, int.class), value);
            case LONG -> process.invokeStaticMethod(
                    MethodDescriptor.ofMethod(Long.class, "valueOf", Long.class, long.class), value);
            case FLOAT -> process.invokeStaticMethod(
                    MethodDescriptor.ofMethod(Float.class, "valueOf", Float.class, float.class), value);
            case DOUBLE -> process.invokeStaticMethod(
                    MethodDescriptor.ofMethod(Double.class, "valueOf", Double.class, double.class), value);
            case CHAR -> process.invokeStaticMethod(
                    MethodDescriptor.ofMethod(Character.class, "valueOf", Character.class, char.class), value);
        };
    }

    private ResultHandle unbox(MethodCreator process, ResultHandle value, org.jboss.jandex.PrimitiveType.Primitive primitive) {
        return switch (primitive) {
            case BOOLEAN -> process.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(Boolean.class, "booleanValue", boolean.class), value);
            case BYTE -> process.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(Byte.class, "byteValue", byte.class), value);
            case SHORT -> process.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(Short.class, "shortValue", short.class), value);
            case INT -> process.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(Integer.class, "intValue", int.class), value);
            case LONG -> process.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(Long.class, "longValue", long.class), value);
            case FLOAT -> process.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(Float.class, "floatValue", float.class), value);
            case DOUBLE -> process.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(Double.class, "doubleValue", double.class), value);
            case CHAR -> process.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(Character.class, "charValue", char.class), value);
        };
    }

    private Class<?> boxedType(org.jboss.jandex.PrimitiveType.Primitive primitive) {
        return switch (primitive) {
            case BOOLEAN -> Boolean.class;
            case BYTE -> Byte.class;
            case SHORT -> Short.class;
            case INT -> Integer.class;
            case LONG -> Long.class;
            case FLOAT -> Float.class;
            case DOUBLE -> Double.class;
            case CHAR -> Character.class;
        };
    }

    private String generatedClassName(OperatorBuildItem operator, int idx) {
        String step = operator.step().name() == null ? "Step" : operator.step().name();
        String normalized = step.replaceAll("[^A-Za-z0-9]", "");
        if (normalized.isBlank()) {
            normalized = "Step";
        }
        if (!Character.isLetter(normalized.charAt(0))) {
            normalized = "Step" + normalized;
        }
        return "org.pipelineframework.generated.operator." + normalized + "OperatorInvoker" + idx;
    }

    private record ValidatedOperator(
            ClassInfo classInfo,
            MethodInfo method,
            OperatorBuildItem buildItem) {
    }
}
