package org.pipelineframework.processor.parser;

import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.pipelineframework.processor.ir.StepDefinition;

/** Parser result for the root definition and the local compile-time definition catalog. */
public record ParsedPipelineDefinitionCatalog(
    List<StepDefinition> rootSteps,
    Map<String, List<StepDefinition>> localDefinitions
) {
    public ParsedPipelineDefinitionCatalog {
        rootSteps = rootSteps == null ? List.of() : List.copyOf(rootSteps);
        localDefinitions = localDefinitions == null ? Map.of() : immutableDefinitions(localDefinitions);
    }

    private static Map<String, List<StepDefinition>> immutableDefinitions(Map<String, List<StepDefinition>> values) {
        Map<String, List<StepDefinition>> copy = new LinkedHashMap<>();
        values.forEach((id, steps) -> copy.put(id, steps == null ? List.of() : List.copyOf(steps)));
        return Collections.unmodifiableMap(copy);
    }
}
