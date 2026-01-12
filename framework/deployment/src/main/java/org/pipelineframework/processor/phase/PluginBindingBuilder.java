package org.pipelineframework.processor.phase;

import java.util.List;
import java.util.Set;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;

import com.squareup.javapoet.ClassName;
import org.pipelineframework.annotation.PipelinePlugin;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.PipelineAspectModel;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.TypeMapping;
import org.pipelineframework.processor.util.AnnotationProcessingUtils;

/**
 * Builds plugin-specific bindings and host steps.
 */
class PluginBindingBuilder {

    /**
     * Builds plugin host steps from aspects and plugin aspect names.
     *
     * @param aspectsForExpansion the list of aspects for expansion
     * @param pluginAspectNames the set of plugin aspect names
     * @return a list of resolved steps for plugin host
     */
    static List<org.pipelineframework.processor.ResolvedStep> buildPluginHostSteps(
            List<PipelineAspectModel> aspectsForExpansion,
            Set<String> pluginAspectNames) {
        if (aspectsForExpansion.isEmpty()) {
            return List.of();
        }
        if (pluginAspectNames == null || pluginAspectNames.isEmpty()) {
            return List.of();
        }

        // This would need access to the context's module directory and config
        // For now, returning an empty list
        return List.of();
    }

    /**
     * Builds a single plugin host step.
     *
     * @param basePackage the base package
     * @param typeName the type name
     * @param aspect the pipeline aspect model
     * @return a resolved step for the plugin host
     */
    static org.pipelineframework.processor.ResolvedStep buildPluginHostStep(
            String basePackage,
            String typeName,
            PipelineAspectModel aspect) {
        Object pluginClassValue = aspect.config().get("pluginImplementationClass");
        if (pluginClassValue == null) {
            return null;
        }

        String pluginImplementationClass = String.valueOf(pluginClassValue);
        ClassName pluginClassName = ClassName.bestGuess(pluginImplementationClass);

        String serviceName = "Observe" + NamingPolicy.toPascalCase(aspect.name()) + typeName + "SideEffectService";
        String generatedName = NamingPolicy.toPascalCase(aspect.name()) + typeName + "SideEffect";

        ClassName domainType = ClassName.get(basePackage + ".common.domain", typeName);
        ClassName mapperType = ClassName.get(basePackage + ".common.mapper", typeName + "Mapper");
        TypeMapping mapping = new TypeMapping(domainType, mapperType, true);

        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName(serviceName)
            .generatedName(generatedName)
            .servicePackage(basePackage + ".service")
            .serviceClassName(pluginClassName)
            .inputMapping(mapping)
            .outputMapping(mapping)
            .streamingShape(org.pipelineframework.processor.ir.StreamingShape.UNARY_UNARY)
            .executionMode(org.pipelineframework.processor.ir.ExecutionMode.DEFAULT)
            .enabledTargets(Set.of()) // This would be resolved based on transport mode
            .deploymentRole(org.pipelineframework.processor.ir.DeploymentRole.PLUGIN_SERVER)
            .sideEffect(true)
            .build();

        return new org.pipelineframework.processor.ResolvedStep(model, null, null);
    }

    /**
     * Extracts plugin aspect names from the compilation context.
     *
     * @param ctx the compilation context
     * @return a set of plugin aspect names
     */
    static Set<String> extractPluginAspectNames(PipelineCompilationContext ctx) {
        Set<String> pluginAspectNames = new java.util.LinkedHashSet<>();
        
        if (ctx.getRoundEnv() != null) {
            Set<? extends Element> pluginElements = ctx.getRoundEnv().getElementsAnnotatedWith(PipelinePlugin.class);
            for (Element element : pluginElements) {
                AnnotationMirror annotationMirror = 
                    AnnotationProcessingUtils.getAnnotationMirror(element, PipelinePlugin.class);
                if (annotationMirror == null) {
                    continue;
                }
                String aspectName = AnnotationProcessingUtils.getAnnotationValueAsString(annotationMirror, "value", null);
                if (aspectName != null && !aspectName.isBlank()) {
                    pluginAspectNames.add(aspectName.trim());
                }
            }
        }
        
        return pluginAspectNames;
    }
}