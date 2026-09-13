package org.pipelineframework.processor.phase;

import com.squareup.javapoet.ClassName;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.ServiceApiKind;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;
import org.pipelineframework.processor.representation.ProviderArtifactWriter;
import org.pipelineframework.processor.representation.ResolvedProviderBoundary;
import org.pipelineframework.representation.spi.ArtifactDescription;
import org.pipelineframework.representation.spi.ProviderGenerationRequest;

/**
 * Materializes artifacts that a provider has described after core has extracted the source contract.
 * The JSR-269 host is the only writer and retains ownership of the semantic pipeline model.
 */
public final class RepresentationProviderGenerationPhase implements PipelineCompilationPhase {
    private final ProviderArtifactWriter artifactWriter;

    public RepresentationProviderGenerationPhase() {
        this(new ProviderArtifactWriter());
    }

    RepresentationProviderGenerationPhase(ProviderArtifactWriter artifactWriter) {
        this.artifactWriter = artifactWriter;
    }

    @Override
    public String name() {
        return "Representation Provider Generation Phase";
    }

    @Override
    public void execute(PipelineCompilationContext ctx) throws Exception {
        if (!(ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig config) || config.version() != 3
                || ctx.getRepresentationProviderRegistry() == null) {
            return;
        }
        List<ResolvedProviderBoundary> boundaries = List.copyOf(ctx.getResolvedProviderBoundaries());
        List<ArtifactDescription> artifacts = new ArrayList<>();
        for (ResolvedProviderBoundary boundary : boundaries) {
            var provider = ctx.getRepresentationProviderRegistry().provider(boundary.claim().providerKey());
            artifacts.addAll(provider.describeArtifacts(new ProviderGenerationRequest(
                boundary.boundary(), boundary.claim(), boundary.representations(), boundary.configuration())));
        }
        artifactWriter.write(ctx.getProcessingEnv().getFiler(), artifacts);
        for (ResolvedProviderBoundary boundary : boundaries) {
            replaceServiceWithFacade(ctx, boundary);
        }
    }

    static void replaceServiceWithFacade(PipelineCompilationContext ctx, ResolvedProviderBoundary boundary) {
        ClassName facade = ClassName.bestGuess(boundary.claim().generatedFacadeTypeName());
        var contract = boundary.claim().stepContract().orElseThrow(() -> new IllegalStateException(
            "Representation provider '" + boundary.claim().providerKey() + "' claimed boundary '"
                + boundary.boundary().stepName() + "' without a generated facade contract."));
        ServiceApiKind facadeApiKind = ServiceApiKind.valueOf(contract.executionStyle().name());
        StreamingShape facadeStreamingShape = StreamingShape.valueOf(contract.cardinality());
        List<PipelineStepModel> updated = ctx.getStepModels().stream()
            .map(model -> isBoundaryModel(model, boundary)
                ? providerFacadeModel(model, facade, facadeApiKind, facadeStreamingShape, boundary)
                : model)
            .toList();
        ctx.setStepModels(updated);
        Map<String, List<PipelineStepModel>> scoped = new LinkedHashMap<>();
        ctx.getLocalDefinitionStepModels().forEach((definition, models) -> scoped.put(definition,
            definition.equals(boundary.definition().logicalId())
                ? models.stream().map(model -> isBoundaryModel(model, boundary)
                    ? providerFacadeModel(model, facade, facadeApiKind, facadeStreamingShape, boundary)
                    : model).toList()
                : models));
        ctx.setLocalDefinitionStepModels(Map.copyOf(scoped));
    }

    static boolean isBoundaryModel(PipelineStepModel model, ResolvedProviderBoundary boundary) {
        String formatted = NamingPolicy.formatForClassName(
            NamingPolicy.stripProcessPrefix(boundary.boundary().stepName()));
        boolean matchingStep = formatted == null || formatted.isBlank()
            ? "ProcessStepService".equals(model.serviceName())
            : formatted.equals(model.serviceName())
                || ("Process" + formatted + "Service").equals(model.serviceName())
                || (formatted + "Service").equals(model.serviceName());
        return boundary.definition().equals(model.definition())
            && matchingStep
            && boundary.boundary().serviceTypeName().equals(model.serviceClassName().canonicalName());
    }

    private static PipelineStepModel providerFacadeModel(
        PipelineStepModel model,
        ClassName facade,
        ServiceApiKind facadeApiKind,
        StreamingShape facadeStreamingShape,
        ResolvedProviderBoundary boundary
    ) {
        return model.withProviderFacade(facade, facadeApiKind, facadeStreamingShape).toBuilder()
            .inputMapping(TypeMapping.withoutMapper(
                ClassName.bestGuess(boundary.boundary().inputType().targetTypeName())))
            .outputMapping(TypeMapping.withoutMapper(
                ClassName.bestGuess(boundary.boundary().outputType().targetTypeName())))
            .build();
    }
}
