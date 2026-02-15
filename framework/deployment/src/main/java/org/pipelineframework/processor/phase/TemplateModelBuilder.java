package org.pipelineframework.processor.phase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import com.squareup.javapoet.ClassName;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateStep;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.TypeMapping;

/**
 * Builds PipelineStepModel instances from pipeline template configuration.
 * Extracted from ModelExtractionPhase.
 */
class TemplateModelBuilder {

    /**
     * Build pipeline step models from the provided template configuration.
     *
     * @param config the pipeline template configuration
     * @return a list of step models derived from the template; empty if no models could be produced
     */
    List<PipelineStepModel> buildModels(PipelineTemplateConfig config) {
        if (config == null || config.steps() == null || config.steps().isEmpty()) {
            return List.of();
        }

        String basePackage = config.basePackage();
        if (basePackage == null || basePackage.isBlank()) {
            return List.of();
        }

        List<PipelineStepModel> models = new ArrayList<>();
        for (PipelineTemplateStep step : config.steps()) {
            if (step == null) {
                continue;
            }

            String stepName = step.name();
            String formattedName = NamingPolicy.formatForClassName(NamingPolicy.stripProcessPrefix(stepName));
            if (formattedName == null || formattedName.isBlank()) {
                continue;
            }

            String serviceName = "Process" + formattedName + "Service";
            String serviceNameForPackage = toPackageSegment(stepName);
            String servicePackage = basePackage + "." + serviceNameForPackage + ".service";

            String inputType = step.inputTypeName();
            String outputType = step.outputTypeName();
            TypeMapping inputMapping = buildMapping(basePackage, inputType);
            TypeMapping outputMapping = buildMapping(basePackage, outputType);

            PipelineStepModel model = new PipelineStepModel.Builder()
                .serviceName(serviceName)
                .servicePackage(servicePackage)
                .serviceClassName(ClassName.get(servicePackage, serviceName))
                .inputMapping(inputMapping)
                .outputMapping(outputMapping)
                .streamingShape(StreamingShapeResolver.streamingShape(step.cardinality()))
                .executionMode(ExecutionMode.DEFAULT)
                .deploymentRole(DeploymentRole.PIPELINE_SERVER)
                .build();

            models.add(model);
        }

        return models;
    }

    private TypeMapping buildMapping(String basePackage, String typeName) {
        if (typeName == null || typeName.isBlank()) {
            return new TypeMapping(null, null, false);
        }
        ClassName domainType = ClassName.get(basePackage + ".common.domain", typeName);
        ClassName mapperType = ClassName.get(basePackage + ".common.mapper", typeName + "Mapper");
        return new TypeMapping(domainType, mapperType, true);
    }

    String toPackageSegment(String name) {
        if (name == null || name.isBlank()) {
            return "service";
        }
        String normalized = name.trim().toLowerCase(Locale.ROOT);
        String sanitized = normalized.replaceAll("[^a-z0-9]+", "_");
        if (sanitized.startsWith("_")) {
            sanitized = sanitized.substring(1);
        }
        if (sanitized.endsWith("_")) {
            sanitized = sanitized.substring(0, sanitized.length() - 1);
        }
        return sanitized.isBlank() ? "service" : sanitized;
    }
}
