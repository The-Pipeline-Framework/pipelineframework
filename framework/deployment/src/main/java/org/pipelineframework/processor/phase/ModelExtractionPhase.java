package org.pipelineframework.processor.phase;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;

import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.extractor.PipelineStepIRExtractor;
import org.pipelineframework.processor.ir.PipelineStepModel;

/**
 * Extracts semantic models from annotated elements.
 * This phase discovers and extracts PipelineStepModel instances from @PipelineStep annotated classes.
 */
public class ModelExtractionPhase implements PipelineCompilationPhase {

    private final TemplateModelBuilder templateModelBuilder;
    private final TemplateExpansionOrchestrator templateExpansionOrchestrator;

    /**
     * Creates a new ModelExtractionPhase with default collaborators.
     */
    public ModelExtractionPhase() {
        this(new TemplateModelBuilder(), new TemplateExpansionOrchestrator());
    }

    /**
     * Creates a new ModelExtractionPhase with explicit collaborators.
     */
    ModelExtractionPhase(TemplateModelBuilder templateModelBuilder,
                         TemplateExpansionOrchestrator templateExpansionOrchestrator) {
        this.templateModelBuilder = Objects.requireNonNull(templateModelBuilder, "templateModelBuilder must not be null");
        this.templateExpansionOrchestrator = Objects.requireNonNull(templateExpansionOrchestrator, "templateExpansionOrchestrator must not be null");
    }

    @Override
    public String name() {
        return "Model Extraction Phase";
    }

    @Override
    public void execute(PipelineCompilationContext ctx) throws Exception {
        List<PipelineStepModel> stepModels = extractStepModels(ctx);

        // Build template models and expand with aspects/plugins
        PipelineTemplateConfig config = ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig cfg ? cfg : null;
        List<PipelineStepModel> baseModels = templateModelBuilder.buildModels(config);
        List<PipelineStepModel> templateModels = templateExpansionOrchestrator.expandTemplateModels(ctx, baseModels);
        if (!templateModels.isEmpty()) {
            stepModels.addAll(templateModels);
        }

        ctx.setStepModels(deduplicateByServiceName(stepModels));
    }

    private List<PipelineStepModel> extractStepModels(PipelineCompilationContext ctx) {
        Set<? extends Element> pipelineStepElements =
            ctx.getRoundEnv().getElementsAnnotatedWith(PipelineStep.class);

        List<PipelineStepModel> stepModels = new ArrayList<>();
        PipelineStepIRExtractor irExtractor = new PipelineStepIRExtractor(ctx.getProcessingEnv());

        for (Element annotatedElement : pipelineStepElements) {
            if (annotatedElement.getKind() != ElementKind.CLASS) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.ERROR,
                    "@PipelineStep can only be applied to classes", annotatedElement);
                continue;
            }

            TypeElement serviceClass = (TypeElement) annotatedElement;
            var result = irExtractor.extract(serviceClass);
            if (result == null) {
                continue;
            }

            stepModels.add(result.model());
        }

        return stepModels;
    }

    private List<PipelineStepModel> deduplicateByServiceName(List<PipelineStepModel> stepModels) {
        Map<String, PipelineStepModel> uniqueByServiceName = new LinkedHashMap<>();
        for (PipelineStepModel model : stepModels) {
            // Keep first occurrence so concrete @PipelineStep models take precedence over template fallbacks.
            uniqueByServiceName.putIfAbsent(model.serviceName(), model);
        }
        return new ArrayList<>(uniqueByServiceName.values());
    }
}
