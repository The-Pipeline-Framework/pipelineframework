package org.pipelineframework.processor.phase;

import java.util.List;

import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.mapping.PipelineRuntimeMapping;
import org.pipelineframework.processor.mapping.PipelineRuntimeMappingResolution;
import org.pipelineframework.processor.mapping.PipelineRuntimeMappingResolver;
import org.jboss.logging.Logger;

/**
 * Resolves runtime mapping placement and filters step models for the current module.
 */
public class PipelineRuntimeMappingPhase implements PipelineCompilationPhase {

    private static final Logger LOG = Logger.getLogger(PipelineRuntimeMappingPhase.class);

    /**
     * Creates a new PipelineRuntimeMappingPhase.
     */
    public PipelineRuntimeMappingPhase() {
    }

    /**
     * Provides the human-readable name of this compilation phase.
     *
     * @return the phase name "Pipeline Runtime Mapping Phase".
     */
    @Override
    public String name() {
        return "Pipeline Runtime Mapping Phase";
    }

    /**
     * Applies runtime module-to-step mapping to the given compilation context, storing the resolved mapping
     * and filtering the context's step models to those applicable to the current module.
     *
     * <p>The method resolves the runtime mapping from the context and saves the resolution back into the
     * context. If the context does not have a module name and the mapping's validation is STRICT, an
     * IllegalStateException is thrown; otherwise a warning is emitted and processing stops without
     * filtering. When a module name is present, step models are filtered so that steps assigned to the
     * current module are kept; steps with no assignment and steps with roles ORCHESTRATOR_CLIENT or
     * PLUGIN_CLIENT are always kept.</p>
     *
     * @param ctx the compilation context to read mapping and step models from and to update with the
     *            resolved mapping and filtered step models
     * @throws IllegalStateException if the mapping requires a module name (STRICT validation) but none
     *                               is provided in the context
     */
    @Override
    public void execute(PipelineCompilationContext ctx) throws Exception {
        PipelineRuntimeMapping mapping = ctx.getRuntimeMapping();
        if (mapping == null) {
            return;
        }

        PipelineRuntimeMappingResolver resolver =
            new PipelineRuntimeMappingResolver(mapping, ctx.getProcessingEnv());
        PipelineRuntimeMappingResolution resolution = resolver.resolve(ctx.getStepModels());
        ctx.setRuntimeMappingResolution(resolution);

        String moduleName = ctx.getModuleName();
        if (moduleName == null || moduleName.isBlank()) {
            if (mapping.validation() == PipelineRuntimeMapping.Validation.STRICT) {
                throw new IllegalStateException("pipeline.module is required when runtime mapping is enabled");
            }
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                    "pipeline.module not provided; runtime mapping is ignored for this module");
            }
            return;
        }

        int originalCount = ctx.getStepModels().size();
        List<PipelineStepModel> filtered = ctx.getStepModels().stream()
            .filter(model -> shouldInclude(model, moduleName, resolution))
            .toList();

        if (filtered.size() != originalCount) {
            LOG.debugf("Runtime mapping filtered step models for module '%s': %d -> %d",
                moduleName, originalCount, filtered.size());
        }
        ctx.setStepModels(filtered);
    }

    /**
     * Decides whether a pipeline step model applies to the current module based on its deployment role and the runtime mapping resolution.
     *
     * @param model the pipeline step model to evaluate
     * @param moduleName the name of the current module
     * @param resolution the resolved runtime mapping containing module assignments for step models
     * @return `true` if the step should be included for the given module; `false` otherwise
     *         (always includes steps with roles ORCHESTRATOR_CLIENT or PLUGIN_CLIENT;
     *         includes unassigned steps; otherwise includes only when the assigned module matches `moduleName`)
     */
    private boolean shouldInclude(
        PipelineStepModel model,
        String moduleName,
        PipelineRuntimeMappingResolution resolution
    ) {
        DeploymentRole role = model.deploymentRole();
        if (role == DeploymentRole.ORCHESTRATOR_CLIENT || role == DeploymentRole.PLUGIN_CLIENT) {
            return true;
        }
        String assigned = resolution.moduleAssignments().get(model);
        if (assigned == null || assigned.isBlank()) {
            return true;
        }
        return assigned.equals(moduleName);
    }
}