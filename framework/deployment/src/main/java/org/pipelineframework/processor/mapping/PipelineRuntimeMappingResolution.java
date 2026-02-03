package org.pipelineframework.processor.mapping;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pipelineframework.processor.ir.PipelineStepModel;

/**
 * Resolved module assignments for pipeline steps and synthetics.
 *
 * @param moduleAssignments resolved module for each step model
 * @param clientOverrides client-name to module mapping for orchestrator client wiring
 * @param moduleByServicePackage resolved module per service package (used for synthetic per-step defaults)
 * @param errors validation errors encountered during resolution
 * @param warnings validation warnings encountered during resolution
 * @param usedModules ordered set of modules referenced by the resolution
 */
public record PipelineRuntimeMappingResolution(
    Map<PipelineStepModel, String> moduleAssignments,
    Map<String, String> clientOverrides,
    Map<String, String> moduleByServicePackage,
    List<String> errors,
    List<String> warnings,
    Set<String> usedModules
) {
    public PipelineRuntimeMappingResolution {
        moduleAssignments = moduleAssignments == null ? Map.of() : Map.copyOf(moduleAssignments);
        clientOverrides = clientOverrides == null ? Map.of() : Map.copyOf(clientOverrides);
        moduleByServicePackage = moduleByServicePackage == null ? Map.of() : Map.copyOf(moduleByServicePackage);
        errors = errors == null ? List.of() : List.copyOf(errors);
        warnings = warnings == null ? List.of() : List.copyOf(warnings);
        if (usedModules == null) {
            usedModules = Set.of();
        } else {
            usedModules = Collections.unmodifiableSet(new LinkedHashSet<>(usedModules));
        }
    }

    /**
     * Indicates whether the resolution contains any validation errors.
     *
     * @return `true` if there is at least one validation error, `false` otherwise.
     */
    public boolean hasErrors() {
        return !errors.isEmpty();
    }
}
