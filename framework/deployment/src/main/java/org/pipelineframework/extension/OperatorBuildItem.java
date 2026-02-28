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

import io.quarkus.builder.item.MultiBuildItem;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.Type;

import java.util.Objects;

/**
 * Build item carrying resolved operator metadata for a YAML step.
 */
public final class OperatorBuildItem extends MultiBuildItem {

    private final PipelineConfigBuildItem.StepConfig step;
    private final ClassInfo operatorClass;
    private final MethodInfo method;
    private final Type inputType;
    private final Type normalizedReturnType;
    private final OperatorCategory category;

    /**
     * Create a build item that carries resolved operator metadata for a pipeline YAML step.
     *
     * @param step                 the step configuration this operator belongs to
     * @param operatorClass        Jandex ClassInfo for the operator's declaring class
     * @param method               Jandex MethodInfo for the operator method
     * @param inputType            the resolved input type for the operator method
     * @param normalizedReturnType the resolved (normalized) return type for the operator method
     * @param category             the operator category
     * @throws NullPointerException if any parameter is null
     */
    public OperatorBuildItem(
            PipelineConfigBuildItem.StepConfig step,
            ClassInfo operatorClass,
            MethodInfo method,
            Type inputType,
            Type normalizedReturnType,
            OperatorCategory category) {
        this.step = Objects.requireNonNull(step, "step must not be null");
        this.operatorClass = Objects.requireNonNull(operatorClass, "operatorClass must not be null");
        this.method = Objects.requireNonNull(method, "method must not be null");
        this.inputType = Objects.requireNonNull(inputType, "inputType must not be null");
        this.normalizedReturnType = Objects.requireNonNull(normalizedReturnType, "normalizedReturnType must not be null");
        this.category = Objects.requireNonNull(category, "category must not be null");
    }

    /**
     * Get the pipeline step configuration associated with this operator.
     *
     * @return the PipelineConfigBuildItem.StepConfig for this operator
     */
    public PipelineConfigBuildItem.StepConfig step() {
        return step;
    }

    /**
     * Resolved Jandex ClassInfo for the operator implementation.
     *
     * @return the operator's ClassInfo
     */
    public ClassInfo operatorClass() {
        return operatorClass;
    }

    /**
     * Gets the resolved operator method information.
     *
     * @return the Jandex {@code MethodInfo} describing the operator method
     */
    public MethodInfo method() {
        return method;
    }

    /**
     * The resolved input type of the operator method.
     *
     * @return the operator's input {@link Type}
     */
    public Type inputType() {
        return inputType;
    }

    /**
     * Gets the normalized return type for the resolved operator method.
     *
     * @return the operator method's normalized return type
     */
    public Type normalizedReturnType() {
        return normalizedReturnType;
    }

    /**
     * Retrieve the operator's category.
     *
     * @return the operator's category
     */
    public OperatorCategory category() {
        return category;
    }

    /**
     * Determine whether this OperatorBuildItem is equal to another object based on its identifying metadata.
     *
     * @param obj the object to compare with this instance
     * @return `true` if `obj` is an `OperatorBuildItem` whose step, operator class name, method id, input type id, normalized return type id, and category are equal to this instance; `false` otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof OperatorBuildItem other)) {
            return false;
        }
        return Objects.equals(step, other.step)
                && Objects.equals(operatorClassName(operatorClass), operatorClassName(other.operatorClass))
                && Objects.equals(methodId(method), methodId(other.method))
                && Objects.equals(typeId(inputType), typeId(other.inputType))
                && Objects.equals(typeId(normalizedReturnType), typeId(other.normalizedReturnType))
                && Objects.equals(category, other.category);
    }

    /**
     * Computes a hash code for this build item derived from its identifying fields.
     *
     * @return an int hash code computed from the step, operator class name, method id, input type id, normalized return type id, and category
     */
    @Override
    public int hashCode() {
        return Objects.hash(
                step,
                operatorClassName(operatorClass),
                methodId(method),
                typeId(inputType),
                typeId(normalizedReturnType),
                category);
    }

    /**
     * String representation of this OperatorBuildItem including step, operator class, method,
     * input type, normalized return type, and category.
     *
     * @return a string containing the step, operator class name, method id, input type id,
     *         normalized return type id, and category
     */
    @Override
    public String toString() {
        return "OperatorBuildItem{"
                + "step=" + step
                + ", operatorClass=" + operatorClassName(operatorClass)
                + ", method=" + methodId(method)
                + ", inputType=" + typeId(inputType)
                + ", normalizedReturnType=" + typeId(normalizedReturnType)
                + ", category=" + category
                + '}';
    }

    /**
     * Obtain the name of the operator class represented by the given ClassInfo.
     *
     * @param classInfo class metadata to extract the name from
     * @return the `DotName` of the class, or `null` if `classInfo` is `null`
     */
    private static DotName operatorClassName(ClassInfo classInfo) {
        return classInfo == null ? null : classInfo.name();
    }

    /**
     * Create a stable identifier string for a method including its declaring class, name, parameter types, and return type.
     *
     * @param methodInfo the method to identify; may be {@code null}
     * @return {@code null} if {@code methodInfo} is {@code null}; otherwise a string in the form
     *         {@code DeclaringClass#methodName(paramType1,paramType2,...):returnType}
     */
    private static String methodId(MethodInfo methodInfo) {
        if (methodInfo == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(methodInfo.declaringClass()).append('#').append(methodInfo.name()).append('(');
        for (int i = 0; i < methodInfo.parametersCount(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(typeId(methodInfo.parameterType(i)));
        }
        sb.append("):").append(typeId(methodInfo.returnType()));
        return sb.toString();
    }

    /**
     * Produces a textual identifier for the given Jandex Type.
     *
     * @param type the Jandex Type to identify, may be null
     * @return the textual identifier for the type, or {@code null} if {@code type} is null
     */
    private static String typeId(Type type) {
        return type == null ? null : type.toString();
    }
}
