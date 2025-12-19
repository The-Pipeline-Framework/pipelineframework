package org.pipelineframework.processor.extractor;

import java.util.EnumSet;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.processor.ir.*;

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
     * Finds the AnnotationMirror instance for a specific annotation present on an element.
     *
     * @param element the element to inspect for the annotation
     * @param annotationClass the annotation class to look for
     * @return the matching {@link AnnotationMirror} if the annotation is present on the element, or {@code null} if not found
     */
    private AnnotationMirror getAnnotationMirror(Element element, Class<?> annotationClass) {
        String annotationClassName = annotationClass.getCanonicalName();
        for (AnnotationMirror annotationMirror : element.getAnnotationMirrors()) {
            if (annotationMirror.getAnnotationType().toString().equals(annotationClassName)) {
                return annotationMirror;
            }
        }
        return null;
    }

    /**
     * Extracts a TypeMirror value from an annotation by member name.
     *
     * @param annotation The annotation mirror to extract the value from
     * @param memberName The name of the annotation member to extract
     * @return The TypeMirror value of the annotation member, or null if not found or if it's a void type
     */
    private TypeMirror getAnnotationValue(AnnotationMirror annotation, String memberName) {
        for (ExecutableElement executableElement : annotation.getElementValues().keySet()) {
            if (executableElement.getSimpleName().toString().equals(memberName)) {
                Object value = annotation.getElementValues().get(executableElement).getValue();
                if (value instanceof TypeMirror) {
                    return (TypeMirror) value;
                } else if (value instanceof String className) {
                    // Handle string values that should be class names
                    if ("void".equals(className) || className.isEmpty() || "java.lang.Void".equals(className)) {
                        // Return null for void types
                        return null;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Extracts a String value from an annotation by member name.
     *
     * @param annotation The annotation mirror to extract the value from
     * @param memberName The name of the annotation member to extract
     * @return The String value of the annotation member, or null if not found
     */
    private String getAnnotationValueAsString(AnnotationMirror annotation, String memberName) {
        for (ExecutableElement executableElement : annotation.getElementValues().keySet()) {
            if (executableElement.getSimpleName().toString().equals(memberName)) {
                Object value = annotation.getElementValues().get(executableElement).getValue();
                if (value instanceof String) {
                    return (String) value;
                } else if (value instanceof TypeMirror) {
                    // For class types, get the qualified name
                    return value.toString();
                }
            }
        }
        return null;
    }

    /**
     * Extracts a boolean value from an annotation by member name.
     *
     * @param annotation The annotation mirror to extract the value from
     * @param memberName The name of the annotation member to extract
     * @param defaultValue The default value to return if the annotation value is not found
     * @return The boolean value of the annotation member, or the default value if not found
     */
    private boolean getAnnotationValueAsBoolean(AnnotationMirror annotation, String memberName, boolean defaultValue) {
        for (ExecutableElement executableElement : annotation.getElementValues().keySet()) {
            if (executableElement.getSimpleName().toString().equals(memberName)) {
                Object value = annotation.getElementValues().get(executableElement).getValue();
                if (value instanceof Boolean) {
                    return (Boolean) value;
                }
                break; // Exit after finding the element even if it's not the correct type
            }
        }
        return defaultValue;
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
        AnnotationMirror annotationMirror = getAnnotationMirror(serviceClass, PipelineStep.class);
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
            getAnnotationValue(annotationMirror, "stepType"));
        
        StepKind stepKind = pipelineStep.local() ? StepKind.LOCAL : StepKind.REMOTE;
        
        Set<GenerationTarget> targets = EnumSet.noneOf(GenerationTarget.class);
        if (getAnnotationValueAsBoolean(annotationMirror, "grpcEnabled", true)) {
            targets.add(GenerationTarget.GRPC_SERVICE);
            if (!pipelineStep.local()) {
                targets.add(GenerationTarget.CLIENT_STEP);
            }
        }
        if (getAnnotationValueAsBoolean(annotationMirror, "restEnabled", false)) {
            targets.add(GenerationTarget.REST_RESOURCE);
        }
        
        // Add plugin targets (always generated for plugin functionality)
        targets.add(GenerationTarget.PLUGIN_ADAPTER);
        targets.add(GenerationTarget.PLUGIN_REACTIVE_SERVICE);
        
        ExecutionMode executionMode = getAnnotationValueAsBoolean(annotationMirror, "runOnVirtualThreads", false) 
            ? ExecutionMode.VIRTUAL_THREADS : ExecutionMode.DEFAULT;

        // Create directional type mappings
        TypeMapping inputMapping = extractTypeMapping(
            getAnnotationValue(annotationMirror, "inputType"),
            getAnnotationValue(annotationMirror, "inputGrpcType"),
            getAnnotationValue(annotationMirror, "inboundMapper"));

        TypeMapping outputMapping = extractTypeMapping(
            getAnnotationValue(annotationMirror, "outputType"),
            getAnnotationValue(annotationMirror, "outputGrpcType"),
            getAnnotationValue(annotationMirror, "outboundMapper"));

        // Extract gRPC implementation and stub types
        TypeMirror grpcImplTypeMirror = getAnnotationValue(annotationMirror, "grpcImpl");
        TypeMirror grpcStubTypeMirror = getAnnotationValue(annotationMirror, "grpcStub");
        String grpcClientName = getAnnotationValueAsString(annotationMirror, "grpcClient");

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
            .restPath(getAnnotationValueAsString(annotationMirror, "path"))
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