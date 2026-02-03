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

    @Override
    public String name() {
        return "Pipeline Runtime Mapping Phase";
    }

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
