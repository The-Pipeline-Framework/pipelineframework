package org.pipelineframework.processor.ir;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.squareup.javapoet.ClassName;
import org.pipelineframework.processor.composition.PipelineReference;

/** Compiler-owned selection for one dynamic operation step and its exact callable catalogue. */
public record DynamicOperationSelection(
    PipelineReference definition,
    String authoredStepName,
    String sourceStepName,
    String runtimeStepId,
    List<CallableSelection> callables
) {
    public DynamicOperationSelection {
        definition = Objects.requireNonNull(definition, "dynamic operation definition must not be null");
        authoredStepName = requireText(authoredStepName, "authored dynamic operation step");
        sourceStepName = requireText(sourceStepName, "dynamic operation source step");
        runtimeStepId = requireText(runtimeStepId, "dynamic operation runtime step ID");
        callables = List.copyOf(Objects.requireNonNull(callables, "dynamic operation callables must not be null"));
        if (callables.isEmpty()) {
            throw new IllegalArgumentException("dynamic operation selection requires at least one callable");
        }
        Map<String, CallableSelection> aliases = new LinkedHashMap<>();
        for (CallableSelection callable : callables) {
            CallableSelection duplicate = aliases.putIfAbsent(callable.alias(), callable);
            if (duplicate != null) {
                throw new IllegalArgumentException("duplicate dynamic callable alias '" + callable.alias() + "'");
            }
        }
    }

    public boolean ownsAuthoredStep(String authoredStepName) {
        return this.authoredStepName.equals(requireText(authoredStepName, "authored dynamic operation step"));
    }

    /** One compile-time-resolved Query or Command target. */
    public record CallableSelection(
        String alias,
        ConnectorOperationSelection operation,
        String inputType,
        ClassName inputClass,
        String outputType,
        ClassName outputClass,
        Map<String, String> trustedArguments,
        String connectorConfigurationDigest
    ) {
        public CallableSelection {
            alias = requireText(alias, "dynamic callable alias");
            operation = Objects.requireNonNull(operation, "dynamic callable operation must not be null");
            inputType = requireText(inputType, "dynamic callable input type");
            inputClass = Objects.requireNonNull(inputClass, "dynamic callable input class must not be null");
            outputType = requireText(outputType, "dynamic callable output type");
            outputClass = Objects.requireNonNull(outputClass, "dynamic callable output class must not be null");
            trustedArguments = immutableStringMap(trustedArguments);
            connectorConfigurationDigest = Objects.requireNonNull(
                connectorConfigurationDigest, "connector configuration digest must not be null");
        }
    }

    private static Map<String, String> immutableStringMap(Map<String, String> source) {
        Objects.requireNonNull(source, "trusted arguments must not be null");
        if (source.isEmpty()) {
            return Map.of();
        }
        Map<String, String> copy = new LinkedHashMap<>();
        source.forEach((key, value) -> copy.put(
            requireText(key, "trusted argument target"),
            requireText(value, "trusted argument source")));
        return Collections.unmodifiableMap(copy);
    }

    private static String requireText(String value, String subject) {
        String normalized = Objects.requireNonNull(value, subject + " must not be null").trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(subject + " must not be blank");
        }
        return normalized;
    }
}
