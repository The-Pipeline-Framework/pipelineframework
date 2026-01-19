package org.pipelineframework.processor.extractor;

import java.util.EnumSet;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.processor.ir.*;
import org.pipelineframework.processor.util.AnnotationProcessingUtils;

/**
 * Extractor that converts PipelineStep annotations to semantic information in PipelineStepModel.
 */
public class PipelineStepIRExtractor {

    private final ProcessingEnvironment processingEnv;

    /**
     * Initialises the extractor with the processing environment used for annotation processing and type utilities.
     *
     * @param processingEnv the ProcessingEnvironment used for annotation processing, messaging and type utilities
     */
    public PipelineStepIRExtractor(ProcessingEnvironment processingEnv) {
        this.processingEnv = processingEnv;
    }

    /**
     * Result class to return the model from the extractor.
     *
     * @param model the extracted pipeline step model
     */
    public record ExtractResult(PipelineStepModel model) {}

    /**
     * Produces a PipelineStepModel by extracting semantic information from a class annotated with `@PipelineStep`.
     *
     * @param serviceClass the element representing the annotated service class
     * @return the extraction result wrapping the constructed PipelineStepModel, or `null` if the annotation mirror could not be obtained
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
        targets.add(GenerationTarget.GRPC_SERVICE);
        targets.add(GenerationTarget.CLIENT_STEP);

        ExecutionMode executionMode = AnnotationProcessingUtils.getAnnotationValueAsBoolean(annotationMirror, "runOnVirtualThreads", false)
            ? ExecutionMode.VIRTUAL_THREADS : ExecutionMode.DEFAULT;
        OrderingRequirement orderingRequirement = AnnotationProcessingUtils.getAnnotationValueAsEnum(
            annotationMirror, "ordering", OrderingRequirement.class, OrderingRequirement.RELAXED);
        ThreadSafety threadSafety = AnnotationProcessingUtils.getAnnotationValueAsEnum(
            annotationMirror, "threadSafety", ThreadSafety.class, ThreadSafety.SAFE);

        // Create directional type mappings
        TypeMapping inputMapping = extractTypeMapping(
            AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "inputType"),
            AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "inboundMapper"));

        TypeMapping outputMapping = extractTypeMapping(
            AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "outputType"),
            AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "outboundMapper"));

        ClassName cacheKeyGenerator = resolveCacheKeyGenerator(annotationMirror);

        String qualifiedServiceName = serviceClass.getQualifiedName().toString();
        ClassName serviceClassName = null;
        try {
            serviceClassName = ClassName.get(serviceClass);
        } catch (Exception e) {
            processingEnv.getMessager().printMessage(
                javax.tools.Diagnostic.Kind.NOTE,
                "Could not obtain ClassName directly, falling back to bestGuess: " + e.getMessage(),
                serviceClass);
            if (qualifiedServiceName != null && !qualifiedServiceName.isBlank()) {
                serviceClassName = ClassName.bestGuess(qualifiedServiceName);
            }
        }
        if (serviceClassName == null) {
            String fallbackName = serviceClass.getSimpleName() != null
                ? serviceClass.getSimpleName().toString()
                : "UnknownService";
            serviceClassName = ClassName.bestGuess(fallbackName);
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
            .orderingRequirement(orderingRequirement)
            .threadSafety(threadSafety)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .cacheKeyGenerator(cacheKeyGenerator)
            .build();

        return new ExtractResult(model);
    }

    /**
     * Create a TypeMapping describing the relationship between a domain type and an optional mapper type.
     *
     * If `domainType` is null or represents `void`/`java.lang.Void`, the result is disabled with no domain or mapper.
     * If `mapperType` is null or represents `void`/`java.lang.Void`, the result contains the domain type, no mapper, and is disabled.
     *
     * @param domainType the domain type to map from; may be null or a `void` type to indicate absence
     * @param mapperType the mapper type to convert the domain type; may be null or a `void` type to indicate no mapper
     * @return a TypeMapping containing resolved `TypeName` values and an `enabled` flag set to `true` only when both domain and mapper are present
     */
    private TypeMapping extractTypeMapping(TypeMirror domainType, TypeMirror mapperType) {
        if (domainType == null
                || domainType.getKind() == javax.lang.model.type.TypeKind.VOID
                || domainType.toString().equals("java.lang.Void")) {
            return new TypeMapping(null, null, false);
        }
        if (mapperType == null
                || mapperType.getKind() == javax.lang.model.type.TypeKind.VOID
                || mapperType.toString().equals("java.lang.Void")) {
            return new TypeMapping(TypeName.get(domainType), null, false);
        }

        return new TypeMapping(
            TypeName.get(domainType),
            TypeName.get(mapperType),
            true
        );
    }

    /**
     * Determine the streaming shape corresponding to a pipeline step type.
     *
     * @param stepType the annotated step type as a TypeMirror (may be null)
     * @return the corresponding StreamingShape; defaults to `UNARY_UNARY` if `stepType` is null or unrecognised.
     *         Recognised mappings:
     *         - `org.pipelineframework.step.StepOneToMany` → `UNARY_STREAMING`
     *         - `org.pipelineframework.step.StepManyToOne` → `STREAMING_UNARY`
     *         - `org.pipelineframework.step.StepManyToMany` → `STREAMING_STREAMING`
     *         - `org.pipelineframework.step.StepOneToOne` → `UNARY_UNARY`
     */
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

    private ClassName resolveCacheKeyGenerator(AnnotationMirror annotationMirror) {
        TypeMirror typeMirror = AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "cacheKeyGenerator");
        if (typeMirror == null) {
            return null;
        }

        TypeElement defaultElement = processingEnv.getElementUtils()
            .getTypeElement("io.quarkus.cache.CacheKeyGenerator");
        if (defaultElement != null && processingEnv.getTypeUtils().isSameType(typeMirror, defaultElement.asType())) {
            return null;
        }

        Element element = processingEnv.getTypeUtils().asElement(typeMirror);
        if (element instanceof TypeElement typeElement) {
            return ClassName.get(typeElement);
        }

        return ClassName.bestGuess(typeMirror.toString());
    }
}
