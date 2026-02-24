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

    public PipelineConfigBuildItem.StepConfig step() {
        return step;
    }

    public ClassInfo operatorClass() {
        return operatorClass;
    }

    public MethodInfo method() {
        return method;
    }

    public Type inputType() {
        return inputType;
    }

    public Type normalizedReturnType() {
        return normalizedReturnType;
    }

    public OperatorCategory category() {
        return category;
    }

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

    private static DotName operatorClassName(ClassInfo classInfo) {
        return classInfo == null ? null : classInfo.name();
    }

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

    private static String typeId(Type type) {
        return type == null ? null : type.toString();
    }
}
