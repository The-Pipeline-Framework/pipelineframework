package org.pipelineframework.processor.extractor;

import java.util.EnumSet;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.processor.ir.*;
import org.pipelineframework.processor.util.AnnotationProcessingUtils;

/**
 * Extractor that converts PipelineStep annotations to semantic information in PipelineStepModel.
 */
public class PipelineStepIRExtractor {

    private final ProcessingEnvironment processingEnv;

    /**
     * Creates a new PipelineStepIRExtractor.
     *
     * @param processingEnv the processing environment
     */
    public PipelineStepIRExtractor(ProcessingEnvironment processingEnv) {
        this.processingEnv = processingEnv;
    }

    /**
     * Result class to return the model from the extractor.
     */
    public record ExtractResult(PipelineStepModel model) {}

    /**
     * Extracts semantic information from a PipelineStep annotation into PipelineStepModel.
     *
     * @param serviceClass the annotated service class element
     * @return the extracted ExtractResult containing model, or null if extraction failed
     */
    public ExtractResult extract(TypeElement serviceClass) {
        // Get the annotation mirror to extract TypeMirror values
        AnnotationMirror annotationMirror = AnnotationProcessingUtils.getAnnotationMirror(serviceClass, PipelineStep.class);
        if (annotationMirror == null) {
            processingEnv.getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Could not get annotation mirror for " + serviceClass,
                serviceClass);
            return null;
        }

        // Determine semantic configuration
        StreamingShape streamingShape = determineStreamingShape(
            AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "stepType"));

        Set<GenerationTarget> targets = EnumSet.noneOf(GenerationTarget.class);
        if (AnnotationProcessingUtils.getAnnotationValueAsBoolean(annotationMirror, "grpcEnabled", true)) {
            targets.add(GenerationTarget.GRPC_SERVICE);
            targets.add(GenerationTarget.CLIENT_STEP);
        }
        if (AnnotationProcessingUtils.getAnnotationValueAsBoolean(annotationMirror, "restEnabled", false)) {
            targets.add(GenerationTarget.REST_RESOURCE);
        }

        ExecutionMode executionMode = AnnotationProcessingUtils.getAnnotationValueAsBoolean(annotationMirror, "runOnVirtualThreads", false)
            ? ExecutionMode.VIRTUAL_THREADS : ExecutionMode.DEFAULT;

        // Create directional type mappings
        TypeMapping inputMapping = extractTypeMapping(
            AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "inputType"),
            AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "inboundMapper"));

        TypeMapping outputMapping = extractTypeMapping(
            AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "outputType"),
            AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "outboundMapper"));

        String qualifiedServiceName = serviceClass.getQualifiedName().toString();
        ClassName serviceClassName;
        try {
            serviceClassName = ClassName.get(serviceClass);
        } catch (Exception e) {
            serviceClassName = null;
        }
        if (serviceClassName == null) {
            serviceClassName = ClassName.bestGuess(qualifiedServiceName);
        }

        // Build the model
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName(serviceClass.getSimpleName().toString())
            .servicePackage(processingEnv.getElementUtils().getPackageOf(serviceClass).getQualifiedName().toString())
            .serviceClassName(serviceClassName)
            .inputMapping(inputMapping)
            .outputMapping(outputMapping)
            .streamingShape(streamingShape)
            .enabledTargets(targets)
            .executionMode(executionMode)
            .build();

        return new ExtractResult(model);
    }

    private TypeMapping extractTypeMapping(TypeMirror domainType, TypeMirror mapperType) {
        if (domainType == null || domainType.toString().equals("void") || domainType.toString().equals("java.lang.Void")) {
            return new TypeMapping(null, null, false);
        }
        if (mapperType == null || mapperType.toString().equals("void") || mapperType.toString().equals("java.lang.Void")) {
            return new TypeMapping(TypeName.get(domainType), null, false);
        }

        return new TypeMapping(
            TypeName.get(domainType),
            TypeName.get(mapperType),
            true
        );
    }

    private StreamingShape determineStreamingShape(TypeMirror stepType) {
        if (stepType != null) {
            String stepTypeStr = stepType.toString();
            switch (stepTypeStr) {
                case "org.pipelineframework.step.StepOneToMany" -> {
                    return StreamingShape.UNARY_STREAMING;
                }
                case "org.pipelineframework.step.StepManyToOne" -> {
                    return StreamingShape.STREAMING_UNARY;
                }
                case "org.pipelineframework.step.StepManyToMany" -> {
                    return StreamingShape.STREAMING_STREAMING;
                }
                case "org.pipelineframework.step.StepOneToOne" -> {
                    return StreamingShape.UNARY_UNARY;
                }
            }
        }
        // Default to UNARY_UNARY for OneToOne
        return StreamingShape.UNARY_UNARY;
    }
}
