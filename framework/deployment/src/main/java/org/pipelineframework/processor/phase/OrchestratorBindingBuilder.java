package org.pipelineframework.processor.phase;

import java.util.List;
import java.util.Set;
import javax.lang.model.element.Element;

import com.squareup.javapoet.ClassName;
import org.pipelineframework.annotation.PipelineOrchestrator;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateStep;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.OrchestratorBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;

/**
 * Builds orchestrator-specific bindings.
 */
class OrchestratorBindingBuilder {

    /**
     * Builds an orchestrator binding from the template config and orchestrator elements.
     *
     * @param config the pipeline template config
     * @param orchestratorElements the set of orchestrator elements
     * @return an orchestrator binding or null if not applicable
     */
    static OrchestratorBinding buildOrchestratorBinding(
            PipelineTemplateConfig config,
            Set<? extends Element> orchestratorElements) {
        if (config == null) {
            return null;
        }
        List<PipelineTemplateStep> steps = config.steps();
        if (steps == null || steps.isEmpty()) {
            return null;
        }
        String basePackage = config.basePackage();
        if (basePackage == null || basePackage.isBlank()) {
            return null;
        }
        PipelineTemplateStep first = steps.getFirst();
        PipelineTemplateStep last = steps.getLast();
        if (first == null || last == null) {
            return null;
        }
        String inputType = first.inputTypeName();
        String outputType = last.outputTypeName();
        if (inputType == null || inputType.isBlank() || outputType == null || outputType.isBlank()) {
            return null;
        }

        boolean inputStreaming = StreamingShapeResolver.isStreamingInputCardinality(first.cardinality());
        boolean outputStreaming = inputStreaming;
        for (PipelineTemplateStep step : steps) {
            outputStreaming = StreamingShapeResolver.applyCardinalityToStreaming(step.cardinality(), outputStreaming);
        }

        String firstServiceNameFormatted = NamingPolicy.formatForClassName(NamingPolicy.stripProcessPrefix(first.name()));
        String firstServiceName = "Process" + firstServiceNameFormatted + "Service";
        StreamingShape firstStreamingShape = StreamingShapeResolver.streamingShape(first.cardinality());

        PipelineStepModel model = new PipelineStepModel(
            "OrchestratorService",
            "OrchestratorService",
            basePackage + ".orchestrator.service",
            ClassName.get(basePackage + ".orchestrator.service", "OrchestratorService"),
            null,
            null,
            StreamingShapeResolver.streamingShape(inputStreaming, outputStreaming),
            Set.of(GenerationTarget.GRPC_SERVICE),
            org.pipelineframework.processor.ir.ExecutionMode.DEFAULT,
            org.pipelineframework.processor.ir.DeploymentRole.ORCHESTRATOR_CLIENT,
            false,
            null
        );

        PipelineOrchestrator orchestratorAnnotation = resolveOrchestratorAnnotation(orchestratorElements);
        String cliName = orchestratorAnnotation == null ? null : NamingPolicy.emptyToNull(orchestratorAnnotation.name());
        String cliDescription = orchestratorAnnotation == null ? null : NamingPolicy.emptyToNull(orchestratorAnnotation.description());
        String cliVersion = orchestratorAnnotation == null ? null : NamingPolicy.emptyToNull(orchestratorAnnotation.version());

        return new OrchestratorBinding(
            model,
            basePackage,
            config.transport(),
            inputType,
            outputType,
            inputStreaming,
            outputStreaming,
            firstServiceName,
            firstStreamingShape,
            cliName,
            cliDescription,
            cliVersion
        );
    }

    /**
     * Resolves the orchestrator annotation from the provided elements.
     *
     * @param orchestratorElements the set of orchestrator elements
     * @return the orchestrator annotation or null
     */
    static PipelineOrchestrator resolveOrchestratorAnnotation(Set<? extends Element> orchestratorElements) {
        if (orchestratorElements == null || orchestratorElements.isEmpty()) {
            return null;
        }
        for (Element element : orchestratorElements) {
            PipelineOrchestrator annotation = element.getAnnotation(PipelineOrchestrator.class);
            if (annotation != null) {
                return annotation;
            }
        }
        return null;
    }
}