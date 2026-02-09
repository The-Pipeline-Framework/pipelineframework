package org.pipelineframework.processor.util;

import java.io.IOException;
import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.StandardLocation;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.pipelineframework.processor.PipelineCompilationContext;

/**
 * Writes deployment platform metadata for generated pipeline artifacts.
 */
public class PipelinePlatformMetadataGenerator {

    private static final String RESOURCE_PATH = "META-INF/pipeline/platform.json";
    private final ProcessingEnvironment processingEnv;
    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    /**
     * Creates a new metadata generator.
     *
     * @param processingEnv processing environment used to write class-output resources
     */
    public PipelinePlatformMetadataGenerator(ProcessingEnvironment processingEnv) {
        this.processingEnv = processingEnv;
    }

    /**
     * Writes platform metadata to META-INF/pipeline/platform.json.
     *
     * @param ctx compilation context
     * @throws IOException when writing the resource fails
     */
    public void writePlatformMetadata(PipelineCompilationContext ctx) throws IOException {
        if (processingEnv == null || ctx == null) {
            return;
        }
        PlatformMetadata metadata = new PlatformMetadata(
            ctx.getPlatformMode() != null ? ctx.getPlatformMode().name() : "STANDARD",
            ctx.getTransportMode() != null ? ctx.getTransportMode().name() : "GRPC",
            ctx.getModuleName(),
            ctx.isPluginHost());

        javax.tools.FileObject resourceFile = processingEnv.getFiler()
            .createResource(StandardLocation.CLASS_OUTPUT, "", RESOURCE_PATH, (javax.lang.model.element.Element[]) null);
        try (var writer = resourceFile.openWriter()) {
            writer.write(gson.toJson(metadata));
        }
    }

    private static final class PlatformMetadata {
        String platform;
        String transport;
        String module;
        boolean pluginHost;

        private PlatformMetadata(String platform, String transport, String module, boolean pluginHost) {
            this.platform = platform;
            this.transport = transport;
            this.module = module;
            this.pluginHost = pluginHost;
        }
    }
}
