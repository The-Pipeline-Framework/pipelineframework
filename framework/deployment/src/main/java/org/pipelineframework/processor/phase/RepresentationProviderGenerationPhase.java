package org.pipelineframework.processor.phase;

import com.squareup.javapoet.ClassName;
import java.util.ArrayList;
import java.util.List;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.ir.PipelineStepModel;
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

    private static void replaceServiceWithFacade(PipelineCompilationContext ctx, ResolvedProviderBoundary boundary) {
        String serviceType = boundary.boundary().serviceTypeName();
        ClassName facade = ClassName.bestGuess(boundary.claim().generatedFacadeTypeName());
        boolean replaced = ctx.getStepModels().stream()
            .anyMatch(model -> serviceType.equals(model.serviceClassName().canonicalName()));
        if (!replaced) {
            return;
        }
        List<PipelineStepModel> updated = ctx.getStepModels().stream()
            .map(model -> serviceType.equals(model.serviceClassName().canonicalName())
                ? model.withServiceClassName(facade)
                : model)
            .toList();
        ctx.setStepModels(updated);
    }
}
