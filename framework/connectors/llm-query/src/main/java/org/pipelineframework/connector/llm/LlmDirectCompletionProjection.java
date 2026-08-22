package org.pipelineframework.connector.llm;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/** Validates and materializes the trusted-input projection around one model-authored field. */
final class LlmDirectCompletionProjection {
    private final Class<?> outputType;
    private final LlmDirectCompletionConfiguration configuration;
    private final RecordComponent completionComponent;

    LlmDirectCompletionProjection(Class<?> outputType, LlmDirectCompletionConfiguration configuration) {
        this.outputType = Objects.requireNonNull(outputType, "LLM output type must not be null");
        this.configuration = Objects.requireNonNull(
            configuration, "LLM direct completion configuration must not be null");
        if (!outputType.isRecord()) {
            throw new IllegalStateException("LLM direct completion projection requires a record output");
        }
        this.completionComponent = component(outputType, configuration.field());
        Set<String> expectedCarry = new LinkedHashSet<>();
        for (RecordComponent component : outputType.getRecordComponents()) {
            if (!component.getName().equals(configuration.field())) {
                expectedCarry.add(component.getName());
            }
        }
        if (!expectedCarry.equals(configuration.carriedFields().keySet())) {
            throw new IllegalStateException(
                "LLM direct completion carry fields must exactly cover output fields " + expectedCarry);
        }
    }

    Class<?> completionType() {
        return completionComponent.getType();
    }

    Object materialize(Object input, Object modelValue) {
        RecordComponent[] components = outputType.getRecordComponents();
        Object[] arguments = new Object[components.length];
        Class<?>[] parameterTypes = new Class<?>[components.length];
        for (int index = 0; index < components.length; index++) {
            RecordComponent component = components[index];
            parameterTypes[index] = component.getType();
            arguments[index] = component.getName().equals(configuration.field())
                ? modelValue
                : readPath(input, configuration.carriedFields().get(component.getName()));
        }
        try {
            return outputType.getDeclaredConstructor(parameterTypes).newInstance(arguments);
        } catch (Exception failure) {
            throw new InvalidModelDecisionException("projected completion output cannot be materialized", failure);
        }
    }

    private static RecordComponent component(Class<?> type, String field) {
        return Arrays.stream(type.getRecordComponents())
            .filter(candidate -> candidate.getName().equals(field))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                "LLM direct completion output has no field '" + field + "'"));
    }

    private static Object readPath(Object input, String path) {
        Object current = Objects.requireNonNull(input, "LLM Query input must not be null");
        for (String segment : path.split("\\.")) {
            if (!current.getClass().isRecord()) {
                throw new InvalidModelDecisionException(
                    "LLM direct completion input path '" + path + "' crosses a non-record value");
            }
            RecordComponent component = Arrays.stream(current.getClass().getRecordComponents())
                .filter(candidate -> candidate.getName().equals(segment))
                .findFirst()
                .orElseThrow(() -> new InvalidModelDecisionException(
                    "LLM direct completion input path '" + path + "' has no field '" + segment + "'"));
            try {
                current = component.getAccessor().invoke(current);
            } catch (IllegalAccessException | InvocationTargetException failure) {
                throw new InvalidModelDecisionException(
                    "LLM direct completion input path '" + path + "' cannot be read", failure);
            }
        }
        return current;
    }
}
