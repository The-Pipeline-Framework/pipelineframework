package org.pipelineframework.processor.util;

import java.io.IOException;
import java.util.Objects;
import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.Diagnostic;
import javax.tools.StandardLocation;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.pipelineframework.processor.PipelineCompilationContext;

/**
 * Writes deployment platform metadata for generated pipeline artifacts.
 */
public class PipelinePlatformMetadataGenerator {

    private static final String RESOURCE_PATH = "META-INF/pipeline/platform.json";
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private final ProcessingEnvironment processingEnv;

    /**
     * Creates a new metadata generator.
     *
     * @param processingEnv processing environment used to write class-output resources
     */
    public PipelinePlatformMetadataGenerator(ProcessingEnvironment processingEnv) {
        this.processingEnv = Objects.requireNonNull(
            processingEnv,
            "processingEnv is required to write platform metadata");
    }

    /**
     * Writes platform metadata to META-INF/pipeline/platform.json.
     *
     * @param ctx compilation context
     * @throws IOException when writing the resource fails
     */
    public void writePlatformMetadata(PipelineCompilationContext ctx) throws IOException {
        if (ctx == null) {
            processingEnv.getMessager().printMessage(
                Diagnostic.Kind.ERROR,
                "Cannot write platform metadata: compilation context is null.");
            throw new IllegalArgumentException("Compilation context is required to write platform metadata");
        }
        PlatformMetadata metadata = new PlatformMetadata(
            ctx.getPlatformMode() != null ? ctx.getPlatformMode().name() : "STANDARD",
            ctx.getTransportMode() != null ? ctx.getTransportMode().name() : "GRPC",
            ctx.getModuleName(),
            ctx.isPluginHost());

        javax.tools.FileObject resourceFile = processingEnv.getFiler()
            .createResource(StandardLocation.CLASS_OUTPUT, "", RESOURCE_PATH);
        try (var writer = resourceFile.openWriter()) {
            writer.write(gson.toJson(metadata));
        }
    }

    private static final class PlatformMetadata {
        private final String platform;
        private final String transport;
        private final String module;
        private final boolean pluginHost;

        private PlatformMetadata(String platform, String transport, String module, boolean pluginHost) {
            this.platform = platform;
            this.transport = transport;
            this.module = module;
            this.pluginHost = pluginHost;
        }
    }
}
