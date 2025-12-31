package org.pipelineframework.processor;

import java.io.IOException;
import java.util.Set;
import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;

import com.google.protobuf.DescriptorProtos;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.processor.extractor.PipelineStepIRExtractor;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.GrpcBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.RestBinding;
import org.pipelineframework.processor.renderer.ClientStepRenderer;
import org.pipelineframework.processor.renderer.GenerationContext;
import org.pipelineframework.processor.renderer.GrpcServiceAdapterRenderer;
import org.pipelineframework.processor.renderer.RestResourceRenderer;
import org.pipelineframework.processor.util.DescriptorFileLocator;
import org.pipelineframework.processor.util.GrpcBindingResolver;
import org.pipelineframework.processor.util.RestBindingResolver;
import org.pipelineframework.processor.util.RoleMetadataGenerator;
import org.pipelineframework.processor.validator.PipelineStepValidator;

/**
 * Java annotation processor that generates both gRPC client and server step implementations
 * based on @PipelineStep annotated service classes.
 * <p>
 * This class now serves as a thin dispatcher that coordinates with specialized
 * components for IR extraction, validation, and code generation.
 */
@SuppressWarnings("unused")
@SupportedAnnotationTypes("org.pipelineframework.annotation.PipelineStep")
@SupportedOptions({
    "protobuf.descriptor.path",  // Optional: path to directory containing descriptor files
    "protobuf.descriptor.file"   // Optional: path to a specific descriptor file
})
@SupportedSourceVersion(SourceVersion.RELEASE_21)
public class PipelineStepProcessor extends AbstractProcessingTool {

    /**
     * Creates a new PipelineStepProcessor.
     *
     * <p>Constructs an instance without configuring internal components; call {@link #init(javax.annotation.processing.ProcessingEnvironment)}
     * with a ProcessingEnvironment before using the processor.</p>
     */
    public PipelineStepProcessor() {
    }

    /**
     * Suffix to append to generated client step classes.
     */
    public static final String CLIENT_STEP_SUFFIX = "ClientStep";

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

    private PipelineStepIRExtractor irExtractor;
    private PipelineStepValidator validator;
    private GrpcServiceAdapterRenderer grpcRenderer;
    private ClientStepRenderer clientRenderer;
    private RestResourceRenderer restRenderer;
    private GrpcBindingResolver bindingResolver;
    private RestBindingResolver restBindingResolver;
    private RoleMetadataGenerator roleMetadataGenerator;

    /**
     * Initialises the processor and its helper components using the provided processing environment.
     *
     * Initialises the IR extractor, validator, gRPC/client/REST renderers, binding resolvers and
     * the role metadata generator so the processor is ready to perform annotation processing.
     *
     * @param processingEnv the processing environment used to configure compiler-facing utilities
     */
    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);

        this.irExtractor = new PipelineStepIRExtractor(processingEnv);
        this.validator = new PipelineStepValidator(processingEnv);
        this.grpcRenderer = new GrpcServiceAdapterRenderer(GenerationTarget.GRPC_SERVICE);
        this.clientRenderer = new ClientStepRenderer(GenerationTarget.CLIENT_STEP);
        this.restRenderer = new RestResourceRenderer();
        this.bindingResolver = new GrpcBindingResolver();
        this.restBindingResolver = new RestBindingResolver();
        this.roleMetadataGenerator = new RoleMetadataGenerator(processingEnv);
    }

    /**
     * Coordinates extraction, validation and artifact generation for elements annotated with {@code @PipelineStep}.
     *
     * <p>For each annotated class this method extracts a pipeline model, validates it, resolves gRPC/REST bindings
     * as required and orchestrates generation of the corresponding artifacts. When processing is finished it writes
     * role metadata.</p>
     *
     * @param annotations the annotation types requested to be processed in this round
     * @param roundEnv the environment for information about the current and prior round
     * @return {@code true} if at least one annotation was processed, {@code false} when no annotations were present
     */
    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (annotations.isEmpty()) {
            // Only write metadata when no more annotations to process (end of last round)
            if (roundEnv.processingOver()) {
                try {
                    roleMetadataGenerator.writeRoleMetadata();
                } catch (IOException e) {
                    processingEnv.getMessager().printMessage(javax.tools.Diagnostic.Kind.ERROR,
                        "Failed to write role metadata: " + e.getMessage());
                }
            }
            return false;
        }

        // Locate and load FileDescriptorSet from protobuf compilation
        DescriptorProtos.FileDescriptorSet descriptorSet = null;
        DescriptorFileLocator descriptorLocator = new DescriptorFileLocator();
        try {
            descriptorSet = descriptorLocator.locateAndLoadDescriptors(processingEnv.getOptions());
        } catch (IOException e) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING,
                "Failed to load protobuf FileDescriptorSet: " + e.getMessage() +
                ". gRPC generation will fail for services that require descriptor-based resolution. " +
                "Please ensure protobuf compilation happens before annotation processing.");
        }

        for (Element annotatedElement : roundEnv.getElementsAnnotatedWith(PipelineStep.class)) {
            if (annotatedElement.getKind() != ElementKind.CLASS) {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                    "@PipelineStep can only be applied to classes", annotatedElement);
                continue;
            }

            TypeElement serviceClass = (TypeElement) annotatedElement;
            PipelineStep pipelineStep = serviceClass.getAnnotation(PipelineStep.class);

            // Extract semantic information into model and binding
            var result = irExtractor.extract(serviceClass);
            if (result == null) {
                continue;
            }
            PipelineStepModel model = result.model();

            // Validate the model for semantic consistency
            if (!validator.validate(model, serviceClass)) {
                continue;
            }

            GrpcBinding grpcBinding = null;
            if (model.enabledTargets().contains(GenerationTarget.GRPC_SERVICE)
                || model.enabledTargets().contains(GenerationTarget.CLIENT_STEP)) {
                try {
                    grpcBinding = bindingResolver.resolve(model, descriptorSet);
                } catch (Exception e) {
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                        "Failed to resolve gRPC binding for service '" + model.serviceName() + "': " + e.getMessage() +
                        ". This indicates a mismatch between the @PipelineStep annotation and the protobuf definition.");
                    continue;
                }
            }

            RestBinding restBinding = null;
            if (model.enabledTargets().contains(GenerationTarget.REST_RESOURCE)) {
                restBinding = restBindingResolver.resolve(model, processingEnv);
            }

            // Generate artifacts based on the resolved bindings
            generateArtifacts(model, grpcBinding, restBinding);
        }

        return true;
    }

    /**
     * Generate Java source artifacts for the given pipeline step model and record their roles.
     *
     * For each target enabled on the model this method creates a source file, delegates rendering
     * to the appropriate renderer (gRPC service, client step or REST resource) and records the
     * generated class with the corresponding role in the role metadata generator.
     *
     * @param model       the pipeline step model containing service/package names and enabled targets
     * @param grpcBinding gRPC binding information used for gRPC and client generation; may be null if not applicable
     * @param restBinding REST binding information used for REST resource generation; may be null if not applicable
     */
    private void generateArtifacts(PipelineStepModel model, GrpcBinding grpcBinding, RestBinding restBinding) {
        for (GenerationTarget target : model.enabledTargets()) {
            try {
                switch (target) {
                    case GRPC_SERVICE:
                        String grpcClassName = model.servicePackage() + PIPELINE_PACKAGE_SUFFIX +
                                "." + model.serviceName() + GRPC_SERVICE_SUFFIX;
                        JavaFileObject grpcFile = processingEnv.getFiler()
                                .createSourceFile(grpcClassName);
                        grpcRenderer.render(grpcBinding, new GenerationContext(processingEnv, grpcFile));
                        roleMetadataGenerator.recordClassWithRole(grpcClassName, "PIPELINE_SERVER");
                        break;
                    case CLIENT_STEP:
                        String clientClassName = model.servicePackage() + PIPELINE_PACKAGE_SUFFIX +
                                "." + model.serviceName().replace("Service", "") +
                                CLIENT_STEP_SUFFIX;
                        JavaFileObject clientFile = processingEnv.getFiler()
                            .createSourceFile(clientClassName);
                        clientRenderer.render(grpcBinding, new GenerationContext(processingEnv, clientFile));
                        // Determine if it's a plugin client or orchestrator client
                        String role = clientClassName.contains("SideEffect") ? "PLUGIN_CLIENT" : "ORCHESTRATOR_CLIENT";
                        roleMetadataGenerator.recordClassWithRole(clientClassName, role);
                        break;
                    case REST_RESOURCE:
                        String restClassName = model.servicePackage() + PIPELINE_PACKAGE_SUFFIX +
                                "." + model.serviceName().replace("Service", "").replace("Reactive", "") +
                                REST_RESOURCE_SUFFIX;
                        JavaFileObject restFile = processingEnv.getFiler()
                            .createSourceFile(restClassName);
                        restRenderer.render(restBinding, new GenerationContext(processingEnv, restFile));
                        roleMetadataGenerator.recordClassWithRole(restClassName, "REST_SERVER");
                        break;
                }
            } catch (IOException e) {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                    "Failed to generate " + target + " for " + model.serviceName() + ": " + e.getMessage());
            }
        }
    }
}