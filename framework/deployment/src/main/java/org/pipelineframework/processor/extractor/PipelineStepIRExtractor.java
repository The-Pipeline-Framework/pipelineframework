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
 * Extractor that converts PipelineStep annotations to semantic information in PipelineStepIR.
 */
public class PipelineStepIRExtractor {

    private ProcessingEnvironment processingEnv;

    /**
     * Creates a new PipelineStepIRExtractor.
     *
     * @param processingEnv the processing environment
     */
    public PipelineStepIRExtractor(ProcessingEnvironment processingEnv) {
        this.processingEnv = processingEnv;
    }

    
    /**
     * Extracts semantic information from a PipelineStep annotation into PipelineStepIR.
     *
     * @param serviceClass the annotated service class element
     * @param pipelineStep the PipelineStep annotation instance
     * @return the extracted PipelineStepIR containing semantic information, or null if extraction failed
     */
    public PipelineStepIR extract(TypeElement serviceClass, PipelineStep pipelineStep) {
        // Get the annotation mirror to extract TypeMirror values
        AnnotationMirror annotationMirror = AnnotationProcessingUtils.getAnnotationMirror(serviceClass, PipelineStep.class);
        if (annotationMirror == null) {
            processingEnv.getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Could not get annotation mirror for " + serviceClass,
                serviceClass);
            // Create a minimal IR with defaults to allow the validator to show the error
            return new PipelineStepIR.Builder()
                .serviceName(serviceClass.getSimpleName().toString())
                .servicePackage(processingEnv.getElementUtils().getPackageOf(serviceClass).getQualifiedName().toString())
                .serviceClassName(com.squareup.javapoet.ClassName.get(serviceClass))
                .inputMapping(new org.pipelineframework.processor.ir.TypeMapping(null, null, null, false))
                .outputMapping(new org.pipelineframework.processor.ir.TypeMapping(null, null, null, false))
                .streamingShape(org.pipelineframework.processor.ir.StreamingShape.UNARY_UNARY)
                .stepKind(org.pipelineframework.processor.ir.StepKind.REMOTE)
                .executionMode(org.pipelineframework.processor.ir.ExecutionMode.DEFAULT)
                .build();
        }

        // Determine semantic configuration
        StreamingShape streamingShape = determineStreamingShape(
            AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "stepType"));
        
        StepKind stepKind = pipelineStep.local() ? StepKind.LOCAL : StepKind.REMOTE;
        
        Set<GenerationTarget> targets = EnumSet.noneOf(GenerationTarget.class);
        if (AnnotationProcessingUtils.getAnnotationValueAsBoolean(annotationMirror, "grpcEnabled", true)) {
            targets.add(GenerationTarget.GRPC_SERVICE);
            if (!pipelineStep.local()) {
                targets.add(GenerationTarget.CLIENT_STEP);
            }
        }
        if (AnnotationProcessingUtils.getAnnotationValueAsBoolean(annotationMirror, "restEnabled", false)) {
            targets.add(GenerationTarget.REST_RESOURCE);
        }
        
        // Add plugin targets (always generated for plugin functionality)
        targets.add(GenerationTarget.PLUGIN_ADAPTER);
        targets.add(GenerationTarget.PLUGIN_REACTIVE_SERVICE);
        
        ExecutionMode executionMode = AnnotationProcessingUtils.getAnnotationValueAsBoolean(annotationMirror, "runOnVirtualThreads", false)
            ? ExecutionMode.VIRTUAL_THREADS : ExecutionMode.DEFAULT;

        // Create directional type mappings
        TypeMapping inputMapping = extractTypeMapping(
            AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "inputType"),
            AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "inputGrpcType"),
            AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "inboundMapper"));

        TypeMapping outputMapping = extractTypeMapping(
            AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "outputType"),
            AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "outputGrpcType"),
            AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "outboundMapper"));

        // Extract gRPC implementation and stub types
        TypeMirror grpcImplTypeMirror = AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "grpcImpl");
        TypeMirror grpcStubTypeMirror = AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "grpcStub");
        String grpcClientName = AnnotationProcessingUtils.getAnnotationValueAsString(annotationMirror, "grpcClient");

        // Extract plugin side effect type
        TypeMirror pluginSideEffectTypeMirror = AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "sideEffect");

        return new PipelineStepIR.Builder()
            .serviceName(serviceClass.getSimpleName().toString())
            .servicePackage(processingEnv.getElementUtils().getPackageOf(serviceClass).getQualifiedName().toString())
            .serviceClassName(ClassName.get(serviceClass))
            .inputMapping(inputMapping)
            .outputMapping(outputMapping)
            .streamingShape(streamingShape)
            .stepKind(stepKind)
            .enabledTargets(targets)
            .executionMode(executionMode)
            .restPath(AnnotationProcessingUtils.getAnnotationValueAsString(annotationMirror, "path"))
            .grpcImplType(grpcImplTypeMirror != null ? TypeName.get(grpcImplTypeMirror) : null)
            .grpcStubType(grpcStubTypeMirror != null ? TypeName.get(grpcStubTypeMirror) : null)
            .grpcClientName(grpcClientName)
            .build();
    }
    
    private TypeMapping extractTypeMapping(TypeMirror domainType, TypeMirror grpcType, TypeMirror mapperType) {
        if (domainType == null || domainType.toString().equals("void") || domainType.toString().equals("java.lang.Void")) {
            return new TypeMapping(null, null, null, false);
        }
        if (grpcType == null || grpcType.toString().equals("void") || grpcType.toString().equals("java.lang.Void")) {
            return new TypeMapping(null, null, null, false);
        }
        if (mapperType == null || mapperType.toString().equals("void") || mapperType.toString().equals("java.lang.Void")) {
            return new TypeMapping(null, null, null, false);
        }

        return new TypeMapping(
            TypeName.get(domainType),
            TypeName.get(grpcType),
            TypeName.get(mapperType),
            true
        );
    }
    
    private StreamingShape determineStreamingShape(TypeMirror stepType) {
        if (stepType != null) {
            String stepTypeStr = stepType.toString();
            if (stepTypeStr.equals("org.pipelineframework.step.StepOneToMany")) {
                return StreamingShape.UNARY_STREAMING;
            } else if (stepTypeStr.equals("org.pipelineframework.step.StepManyToOne")) {
                return StreamingShape.STREAMING_UNARY;
            } else if (stepTypeStr.equals("org.pipelineframework.step.StepManyToMany")) {
                return StreamingShape.STREAMING_STREAMING;
            } else if (stepTypeStr.equals("org.pipelineframework.step.StepOneToOne")) {
                return StreamingShape.UNARY_UNARY;
            }
        }
        // Default to UNARY_UNARY for OneToOne
        return StreamingShape.UNARY_UNARY;
    }
}