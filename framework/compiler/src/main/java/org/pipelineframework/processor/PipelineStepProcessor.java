package org.pipelineframework.processor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedOptions;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;

import org.pipelineframework.annotation.PipelineOrchestrator;
import org.pipelineframework.annotation.PipelinePlugin;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.processor.phase.ModelExtractionPhase;
import org.pipelineframework.processor.phase.OperationRepresentationGenerationPhase;
import org.pipelineframework.processor.phase.PipelineBindingConstructionPhase;
import org.pipelineframework.processor.phase.PipelineBranchPlanningPhase;
import org.pipelineframework.processor.phase.PipelineDiscoveryPhase;
import org.pipelineframework.processor.phase.PipelineGenerationPhase;
import org.pipelineframework.processor.phase.PipelineInfrastructurePhase;
import org.pipelineframework.processor.phase.PipelineRuntimeMappingPhase;
import org.pipelineframework.processor.phase.PipelineSemanticAnalysisPhase;
import org.pipelineframework.processor.phase.PipelineTargetResolutionPhase;
import org.pipelineframework.processor.phase.RepresentationProviderGenerationPhase;
import org.pipelineframework.processor.phase.RepresentationProviderPreparationPhase;
import org.pipelineframework.processor.util.RoleMetadataGenerator;

/**
 * Production JSR-269 host for pipeline compilation.
 *
 * <p>The processor owns annotation-processing lifecycle and discovery signals while delegating
 * semantic and generation work to the ordered {@link PipelineCompiler} phase engine.</p>
 */
@SuppressWarnings("unused")
@SupportedAnnotationTypes({
    "*",
    "org.pipelineframework.annotation.PipelineStep",
    "org.pipelineframework.annotation.PipelinePlugin",
    "org.pipelineframework.annotation.PipelineOrchestrator"
})
@SupportedOptions({
    "protobuf.descriptor.path",  // Optional: path to directory containing descriptor files
    "protobuf.descriptor.file",  // Optional: path to a specific descriptor file
    "pipeline.generatedSourcesDir", // Optional: base directory for role-specific generated sources
    "pipeline.generatedSourcesRoot", // Optional: legacy alias for generated sources base directory
    "pipeline.config", // Optional: explicit pipeline.yaml path
    "pipeline.cache.keyGenerator", // Optional: fully-qualified CacheKeyGenerator class for @CacheResult
    "pipeline.orchestrator.generate", // Optional: enable orchestrator endpoint generation
    "pipeline.warnUnreferencedSteps", // Optional: suppress warnings for intentionally runtime-mapped step services
    "pipeline.module", // Optional: logical module name for runtime mapping
    "pipeline.moduleDir", // Optional: explicit module directory for runtime mapping/config discovery
    "project.basedir", // Optional: project base directory used for config discovery fallback
    "pipeline.function.httpBridge", // Optional: prefer HTTP bridge over generated direct function handlers
    "pipeline.platform", // Optional: target deployment platform (COMPUTE|FUNCTION; legacy: STANDARD|LAMBDA)
    "pipeline.transport", // Optional: transport mode (GRPC|REST|LOCAL)
    "pipeline.rest.naming.strategy", // Optional: REST naming strategy (LEGACY|RESOURCEFUL)
    "pipeline.mapper.fallback.enabled", // Optional: enables delegated mapper fallback engine
    "pipeline.parallelism", // Optional: parallelism mode (PARALLEL|SEQUENTIAL|AUTO)
    "pipeline.codegen.rendererProfile" // Optional: renderer profile selection (quarkus|spring)
})
@SupportedSourceVersion(SourceVersion.RELEASE_21)
public class PipelineStepProcessor extends AbstractProcessingTool {

    /** Suffix to append to generated gRPC client step classes. */
    public static final String GRPC_CLIENT_STEP_SUFFIX = "GrpcClientStep";

    /** Suffix to append to generated REST client step classes. */
    public static final String REST_CLIENT_STEP_SUFFIX = "RestClientStep";

    /** Suffix to append to generated gRPC service classes. */
    public static final String GRPC_SERVICE_SUFFIX = "GrpcService";

    /** Suffix to append to generated REST resource classes. */
    public static final String REST_RESOURCE_SUFFIX = "Resource";

    private final PipelineCompiler compiler;
    private boolean compilationExecuted;

    /** Creates the service-loader compatible production processor. */
    public PipelineStepProcessor() {
        this(new PipelineCompiler(List.of(
            new PipelineDiscoveryPhase(),
            new RepresentationProviderPreparationPhase(),
            new PipelineBranchPlanningPhase(),
            new ModelExtractionPhase(),
            new OperationRepresentationGenerationPhase(),
            new PipelineRuntimeMappingPhase(),
            new RepresentationProviderGenerationPhase(),
            new PipelineSemanticAnalysisPhase(),
            new PipelineTargetResolutionPhase(),
            new PipelineBindingConstructionPhase(),
            new PipelineGenerationPhase(),
            new PipelineInfrastructurePhase()
        )));
    }

    PipelineStepProcessor(PipelineCompiler compiler) {
        this.compiler = Objects.requireNonNull(compiler, "compiler must not be null");
    }

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        compilationExecuted = false;
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (compilationExecuted) {
            return false;
        }

        Set<? extends Element> pipelineStepElements = roundEnv.getElementsAnnotatedWith(PipelineStep.class);
        Set<? extends Element> orchestratorElements = roundEnv.getElementsAnnotatedWith(PipelineOrchestrator.class);
        Set<? extends Element> pluginElements = roundEnv.getElementsAnnotatedWith(PipelinePlugin.class);
        boolean hasRelevantAnnotations = !pipelineStepElements.isEmpty()
            || !orchestratorElements.isEmpty()
            || !pluginElements.isEmpty();

        if (!hasRelevantAnnotations && !hasPipelineConfigSignal()) {
            writeRoleMetadataWhenProcessingCompletes(annotations, roundEnv);
            return false;
        }

        PipelineCompilationContext context = new PipelineCompilationContext(
            processingEnv,
            new Jsr269SourceInventory(
                pipelineStepElements,
                orchestratorElements,
                pluginElements,
                roundEnv.getRootElements()
            ),
            new PipelineCompilerOptions(processingEnv.getOptions()),
            new Jsr269PipelineCompilerDiagnostics(processingEnv.getMessager())
        );
        context.setRepresentationProviderClassLoader(getClass().getClassLoader());
        try {
            compiler.compile(context);
        } catch (PipelineCompilationException failure) {
            emitFailureDiagnostics(failure);
        }
        compilationExecuted = true;
        return false;
    }

    private void writeRoleMetadataWhenProcessingCompletes(
        Set<? extends TypeElement> annotations,
        RoundEnvironment roundEnv
    ) {
        if (!annotations.isEmpty() || !roundEnv.processingOver()) {
            return;
        }
        try {
            new RoleMetadataGenerator(processingEnv).writeRoleMetadata();
        } catch (Exception failure) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING,
                "Failed to write role metadata: " + failure.getMessage());
        }
    }

    private void emitFailureDiagnostics(PipelineCompilationException failure) {
        Throwable phaseFailure = failure.getCause();
        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
            "Pipeline compilation failed in phase '" + failure.phaseName() + "': " + phaseFailure.getMessage());
        Throwable cause = phaseFailure.getCause();
        if (cause != null && cause.getMessage() != null && !cause.getMessage().isBlank()) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,
                "Cause: " + cause.getClass().getSimpleName() + ": " + cause.getMessage());
        }
    }

    private boolean hasPipelineConfigSignal() {
        String configuredPath = processingEnv.getOptions().get("pipeline.config");
        if (configuredPath != null && !configuredPath.isBlank()) {
            Path configured = Path.of(configuredPath.trim());
            if (Files.exists(configured) && Files.isRegularFile(configured) && Files.isReadable(configured)) {
                return true;
            }
            processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING,
                "Ignoring pipeline.config because it is not a readable file: " + configuredPath);
            return false;
        }

        String baseDir = processingEnv.getOptions().get("pipeline.moduleDir");
        if (baseDir == null || baseDir.isBlank()) {
            baseDir = processingEnv.getOptions().get("pipeline.generatedSourcesDir");
        }
        if (baseDir == null || baseDir.isBlank()) {
            baseDir = processingEnv.getOptions().get("project.basedir");
        }
        if (baseDir == null || baseDir.isBlank()) {
            baseDir = System.getProperty("maven.multiModuleProjectDirectory");
        }
        if (baseDir == null || baseDir.isBlank()) {
            return false;
        }

        Path basePath = Path.of(baseDir);
        if (Files.exists(basePath.resolve("pipeline.yaml"))) {
            return true;
        }
        return Files.exists(basePath.resolve(Path.of("src", "main", "resources", "pipeline.yaml")));
    }
}
