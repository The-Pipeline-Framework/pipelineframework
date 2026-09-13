package org.pipelineframework.processor.phase;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.lang.model.element.Element;

import com.squareup.javapoet.ClassName;
import org.pipelineframework.annotation.PipelineOrchestrator;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateStep;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.OrchestratorBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StepDefinition;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;

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
            Set<? extends Element> orchestratorElements,
            String resolvedTransport,
            List<StepDefinition> stepDefinitions) {
        if (config == null) {
            return null;
        }
        Objects.requireNonNull(stepDefinitions, "stepDefinitions must not be null");
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
        TypeMapping inputMapping = rootMapping(stepDefinitions, true, inputType);
        TypeMapping outputMapping = rootMapping(stepDefinitions, false, outputType);

        PipelineStepModel model = new PipelineStepModel(
            "OrchestratorService",
            "OrchestratorService",
            basePackage + ".orchestrator.service",
            ClassName.get(basePackage + ".orchestrator.service", "OrchestratorService"),
            inputMapping,
            outputMapping,
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

        String transport = (resolvedTransport == null || resolvedTransport.isBlank())
            ? config.transport()
            : resolvedTransport;

        return new OrchestratorBinding(
            model,
            basePackage,
            transport,
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

    private static TypeMapping rootMapping(
            List<StepDefinition> stepDefinitions,
            boolean input,
            String canonicalTypeName) {
        Optional<ClassName> javaType = stepDefinitions.isEmpty()
            ? Optional.empty()
            : Optional.ofNullable(input
                ? stepDefinitions.getFirst().inputType()
                : stepDefinitions.getLast().outputType());
        return javaType
            .map(type -> TypeMapping.canonical(type, canonicalTypeName))
            .orElseGet(TypeMapping::unresolved);
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
