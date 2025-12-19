package org.pipelineframework.processor;

import java.io.IOException;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;

import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.processor.extractor.PipelineStepIRExtractor;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepIR;
import org.pipelineframework.processor.renderer.ClientStepRenderer;
import org.pipelineframework.processor.renderer.GenerationContext;
import org.pipelineframework.processor.renderer.GrpcServiceAdapterRenderer;
import org.pipelineframework.processor.renderer.RestResourceRenderer;
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
@SupportedSourceVersion(SourceVersion.RELEASE_21)
public class PipelineStepProcessor extends AbstractProcessingTool {

    /**
     * Creates a new PipelineStepProcessor instance.
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

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        
        this.irExtractor = new PipelineStepIRExtractor(processingEnv);
        this.validator = new PipelineStepValidator(processingEnv);
        this.grpcRenderer = new GrpcServiceAdapterRenderer();
        this.clientRenderer = new ClientStepRenderer();
        this.restRenderer = new RestResourceRenderer();
    }

    /**
     * Processes elements annotated with {@code @PipelineStep} by extracting semantic
     * information into IR, validating it, and coordinating code generation.
     */
    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (annotations.isEmpty()) {
            return false;
        }

        for (Element annotatedElement : roundEnv.getElementsAnnotatedWith(PipelineStep.class)) {
            if (annotatedElement.getKind() != ElementKind.CLASS) {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                    "@PipelineStep can only be applied to classes", annotatedElement);
                continue;
            }

            TypeElement serviceClass = (TypeElement) annotatedElement;
            PipelineStep pipelineStep = serviceClass.getAnnotation(PipelineStep.class);

            // Extract semantic information into IR
            PipelineStepIR ir = irExtractor.extract(serviceClass, pipelineStep);
            
            // Validate the IR for semantic consistency
            if (!validator.validate(ir, serviceClass)) {
                continue;
            }
            
            // Generate artifacts based on the IR
            generateArtifacts(ir);
        }

        return true;
    }
    
    private void generateArtifacts(PipelineStepIR ir) {
        for (GenerationTarget target : ir.getEnabledTargets()) {
            try {
                switch (target) {
                    case GRPC_SERVICE:
                        if (ir.getStepKind() != org.pipelineframework.processor.ir.StepKind.LOCAL) {
                            JavaFileObject grpcFile = processingEnv.getFiler()
                                .createSourceFile(ir.getServicePackage() + PIPELINE_PACKAGE_SUFFIX + 
                                    "." + ir.getServiceName() + GRPC_SERVICE_SUFFIX);
                            grpcRenderer.render(ir, new GenerationContext(processingEnv, grpcFile));
                        }
                        break;
                    case CLIENT_STEP:
                        if (ir.getStepKind() != org.pipelineframework.processor.ir.StepKind.LOCAL) {
                            JavaFileObject clientFile = processingEnv.getFiler()
                                .createSourceFile(ir.getServicePackage() + PIPELINE_PACKAGE_SUFFIX + 
                                    "." + ir.getServiceName().replace("Service", "") + 
                                    CLIENT_STEP_SUFFIX);
                            clientRenderer.render(ir, new GenerationContext(processingEnv, clientFile));
                        }
                        break;
                    case REST_RESOURCE:
                        JavaFileObject restFile = processingEnv.getFiler()
                            .createSourceFile(ir.getServicePackage() + PIPELINE_PACKAGE_SUFFIX + 
                                "." + ir.getServiceName().replace("Service", "").replace("Reactive", "") + 
                                REST_RESOURCE_SUFFIX);
                        restRenderer.render(ir, new GenerationContext(processingEnv, restFile));
                        break;
                }
            } catch (IOException e) {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                    "Failed to generate " + target + " for " + ir.getServiceName() + ": " + e.getMessage());
            }
        }
    }
}