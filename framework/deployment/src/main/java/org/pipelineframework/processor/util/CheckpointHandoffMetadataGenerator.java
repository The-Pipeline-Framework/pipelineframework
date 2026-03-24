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
            true);

        javax.tools.FileObject resourceFile = processingEnv.getFiler()
            .createResource(StandardLocation.CLASS_OUTPUT, "", RESOURCE_PATH);
        try (var writer = resourceFile.openWriter()) {
            writer.write(GSON.toJson(metadata));
        }
    }

    private record HandoffMetadata(
        String outputPublication,
        String inputSubscription,
        String checkpointOutputType,
        String ingressInputType,
        String mapper,
        List<String> idempotencyKeyFields,
        List<String> runtimeIngressCapabilities,
        boolean queueAsyncRequired
    ) {
    }
}
