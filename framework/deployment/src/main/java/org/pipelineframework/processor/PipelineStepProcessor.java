package org.pipelineframework.processor;

import java.util.List;
import java.util.Set;
import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.TypeElement;

/**
 * Java annotation processor that generates both gRPC client and server step implementations
 * based on @PipelineStep annotated service classes.
 * <p>
 * This class now serves as a facade that delegates to the phased compiler architecture.
 */
@SuppressWarnings("unused")
@SupportedAnnotationTypes({
    "org.pipelineframework.annotation.PipelineStep",
    "org.pipelineframework.annotation.PipelinePlugin",
    "org.pipelineframework.annotation.PipelineOrchestrator"
})
@SupportedOptions({
    "protobuf.descriptor.path",  // Optional: path to directory containing descriptor files
    "protobuf.descriptor.file",  // Optional: path to a specific descriptor file
    "pipeline.generatedSourcesDir", // Optional: base directory for role-specific generated sources
    "pipeline.generatedSourcesRoot", // Optional: legacy alias for generated sources base directory
    "pipeline.cache.keyGenerator", // Optional: fully-qualified CacheKeyGenerator class for @CacheResult
    "pipeline.orchestrator.generate" // Optional: enable orchestrator endpoint generation
})
@SupportedSourceVersion(SourceVersion.RELEASE_21)
@Deprecated(forRemoval = true)
public class PipelineStepProcessor extends AbstractProcessingTool {

    private PipelineCompiler compiler;

    /**
     * Suffix to append to generated gRPC client step classes.
     */
    public static final String GRPC_CLIENT_STEP_SUFFIX = "GrpcClientStep";

    /**
     * Suffix to append to generated REST client step classes.
     */
    public static final String REST_CLIENT_STEP_SUFFIX = "RestClientStep";

    /**
     * Suffix to append to generated gRPC service classes.
     */
    public static final String GRPC_SERVICE_SUFFIX = "GrpcService";

    /**
     * Package suffix for generated pipeline classes.
     */
    public static final String PIPELINE_PACKAGE_SUFFIX = ".pipeline";

    /**
     * Suffix to append to generated REST resource classes.
     */
    public static final String REST_RESOURCE_SUFFIX = "Resource";

    /**
     * Creates a new PipelineStepProcessor.
     */
    public PipelineStepProcessor() {
    }

    /**
     * Initialises the processor by creating the phased compiler.
     *
     * @param processingEnv the processing environment used to configure compiler-facing utilities
     */
    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);

        // Create the phased compiler with all the phases
        List<PipelineCompilationPhase> phases = List.of(
            new org.pipelineframework.processor.phase.PipelineDiscoveryPhase(),
            new org.pipelineframework.processor.phase.ModelExtractionPhase(),
            new org.pipelineframework.processor.phase.PipelineSemanticAnalysisPhase(),
            new org.pipelineframework.processor.phase.PipelineTargetResolutionPhase(),
            new org.pipelineframework.processor.phase.PipelineBindingConstructionPhase(),
            new org.pipelineframework.processor.phase.PipelineGenerationPhase(),
            new org.pipelineframework.processor.phase.PipelineInfrastructurePhase()
        );

        this.compiler = new PipelineCompiler(phases);
        this.compiler.init(processingEnv);
    }

    /**
     * Delegates processing to the phased compiler.
     *
     * @param annotations the annotation types requested to be processed in this round
     * @param roundEnv the environment for information about the current and prior round
     * @return {@code true} if at least one annotation was processed, {@code false} when no annotations were present
     */
    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        // Delegate validation and generation to the phased compiler.
        return compiler.process(annotations, roundEnv);
    }
}
