package org.pipelineframework.processor.phase;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.processing.Messager;
import javax.tools.Diagnostic;

import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineOrchestratorModel;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.TransportMode;

/**
 * Evaluates whether gRPC bindings are required for the current compilation.
 * Extracted from PipelineBindingConstructionPhase.
 */
class GrpcRequirementEvaluator {

    /**
     * Determine whether gRPC bindings are required.
     *
     * @param stepModels the pipeline step models
     * @param orchestratorModels the orchestrator models
     * @param templateConfig the pipeline template config, may be null
     * @param messager the messager for diagnostics, may be null
     * @return true if gRPC bindings are required
     */
    boolean needsGrpcBindings(
            List<PipelineStepModel> stepModels,
            List<PipelineOrchestratorModel> orchestratorModels,
            PipelineTemplateConfig templateConfig,
            Messager messager) {
        Objects.requireNonNull(stepModels, "stepModels must not be null");
        Objects.requireNonNull(orchestratorModels, "orchestratorModels must not be null");
        
        if (stepModels.stream().anyMatch(model ->
            model.enabledTargets().contains(GenerationTarget.GRPC_SERVICE)
                || model.enabledTargets().contains(GenerationTarget.CLIENT_STEP))) {
            return true;
        }
        if (!orchestratorModels.isEmpty()) {
            if (templateConfig == null) {
                return false;
            }
            String transport = templateConfig.transport();
            if (transport == null || transport.isBlank()) {
                return true;
            }
            Optional<TransportMode> resolvedMode = TransportMode.fromStringOptional(transport);
            if (resolvedMode.isEmpty()) {
                if (messager != null) {
                    messager.printMessage(Diagnostic.Kind.WARNING,
                        "Unknown transport '" + transport + "' in pipeline template. "
                            + "Valid values are GRPC|gRPC|REST|LOCAL.");
                }
                // Mirror resolver behavior: treat unknown transport as GRPC (return true)
                return true;
            }
            return resolvedMode.get() == TransportMode.GRPC;
        }
        return false;
    }
}
