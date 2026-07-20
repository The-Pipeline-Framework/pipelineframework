package org.pipelineframework.processor.util;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.StandardLocation;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateStep;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeReference;
import org.pipelineframework.config.template.PipelineTemplateUnion;
import org.pipelineframework.branching.BranchVariantIdentity;
import org.pipelineframework.processor.PipelineCompilationContext;

/**
 * Writes logical checkpoint handoff metadata for tooling and exports.
 */
public class CheckpointHandoffMetadataGenerator {

    private static final String RESOURCE_PATH = "META-INF/pipeline/handoff.json";
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    private final ProcessingEnvironment processingEnv;

    public CheckpointHandoffMetadataGenerator(ProcessingEnvironment processingEnv) {
        this.processingEnv = Objects.requireNonNull(processingEnv, "processingEnv is required");
    }

    public void writeHandoffMetadata(PipelineCompilationContext ctx) throws IOException {
        if (ctx == null || !(ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig templateConfig)) {
            return;
        }
        boolean hasInput = templateConfig.input() != null && templateConfig.input().subscription() != null;
        boolean hasOutput = templateConfig.output() != null && templateConfig.output().checkpoint() != null;
        if (!hasInput && !hasOutput) {
            return;
        }
        List<PipelineTemplateStep> steps = templateConfig.steps() == null ? List.of() : templateConfig.steps();
        String ingressInputType = steps.isEmpty() ? null : steps.getFirst().inputTypeName();
        String checkpointOutputType = steps.isEmpty() ? null : steps.getLast().outputTypeName();

        HandoffMetadata metadata = new HandoffMetadata(
            hasOutput ? templateConfig.output().checkpoint().publication() : null,
            hasInput ? templateConfig.input().subscription().publication() : null,
            checkpointOutputType,
            ingressInputType,
            hasInput ? templateConfig.input().subscription().mapper() : null,
            hasOutput ? templateConfig.output().checkpoint().idempotencyKeyFields() : List.of(),
            hasInput ? List.of("HTTP_JSON", "HTTP_PROTO", "GRPC") : List.of(),
            checkpointVariants(templateConfig, checkpointOutputType),
            true);

        javax.tools.FileObject resourceFile = processingEnv.getFiler()
            .createResource(StandardLocation.CLASS_OUTPUT, "", RESOURCE_PATH);
        try (var writer = resourceFile.openWriter()) {
            writer.write(GSON.toJson(metadata));
        }
    }

    private List<BranchVariantIdentity> checkpointVariants(PipelineTemplateConfig templateConfig, String contract) {
        if (contract == null || contract.isBlank()) {
            return List.of();
        }
        if (templateConfig.dialect() == org.pipelineframework.config.template.PipelineTemplateDialect.V3) {
            PipelineTemplateTypeDefinition definition = templateConfig.typeModel().definitions().get(contract);
            if (!(definition instanceof PipelineTemplateTypeDefinition.UnionType union)) {
                return List.of();
            }
            return union.variants().values().stream()
                .map(variant -> checkpointVariant(templateConfig, union.name(), variant))
                .toList();
        }
        PipelineTemplateUnion union = templateConfig.unions().get(contract);
        if (union == null) {
            return List.of();
        }
        return union.variants().entrySet().stream()
            .filter(entry -> entry.getValue() != null && entry.getValue().type() != null)
            .map(entry -> new BranchVariantIdentity(union.name(), entry.getKey(), entry.getValue().type()))
            .toList();
    }

    private BranchVariantIdentity checkpointVariant(
        PipelineTemplateConfig templateConfig,
        String unionName,
        PipelineTemplateTypeDefinition.Variant variant
    ) {
        PipelineTemplateTypeReference payload = templateConfig.typeModel().resolveAliases(variant.payload());
        if (!(payload instanceof PipelineTemplateTypeReference.Named named)) {
            throw new IllegalStateException("Checkpoint union '" + unionName + "' variant '"
                + variant.discriminator() + "' does not resolve to a named payload contract.");
        }
        return new BranchVariantIdentity(unionName, variant.discriminator(), named.name());
    }

    private record HandoffMetadata(
        String outputPublication,
        String inputSubscription,
        String checkpointOutputType,
        String ingressInputType,
        String mapper,
        List<String> idempotencyKeyFields,
        List<String> runtimeIngressCapabilities,
        List<BranchVariantIdentity> checkpointVariants,
        boolean queueAsyncRequired
    ) {
    }
}
