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
 * This extractor reads declared step metadata.
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
     * <p>
     * Mapper inference is NOT performed here. Mapping remains unresolved for later phases.
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

        ExecutionMode executionMode = ExecutionMode.DEFAULT;
        OrderingRequirement orderingRequirement = AnnotationProcessingUtils.getAnnotationValueAsEnum(
            annotationMirror, "ordering", OrderingRequirement.class, OrderingRequirement.RELAXED);
        ThreadSafety threadSafety = AnnotationProcessingUtils.getAnnotationValueAsEnum(
            annotationMirror, "threadSafety", ThreadSafety.class, ThreadSafety.SAFE);

        // Create directional type mappings. Explicit mapper references are retained as backward-compatible
        // fallback while build-time inference can still populate missing mappings in later phases.
        TypeMapping inputMapping = extractTypeMapping(
            AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "inputType"),
            AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "inboundMapper"));

        TypeMapping outputMapping = extractTypeMapping(
            AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "outputType"),
            AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "outboundMapper"));

        ClassName cacheKeyGenerator = resolveCacheKeyGenerator(annotationMirror);
        
        // Extract delegated operator and mapper class names
        ClassName delegateService = resolveDelegateService(annotationMirror);
        ClassName externalMapper = resolveExternalMapper(annotationMirror);

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

        // Build the model - mappers are not yet inferred
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
            .delegateService(delegateService)
            .externalMapper(externalMapper)
            .build();

        return new ExtractResult(model);
    }

    /**
         * Create a TypeMapping that represents a domain type and an optional mapper type.
         *
         * @param domainType the domain type to map from; may be null or the `void`/`java.lang.Void` type to indicate absence
         * @param mapperTypeMirror an optional mapper type mirror from the annotation; may be null or the `void`/`java.lang.Void` type
         * @return a `TypeMapping` containing the resolved domain type, the mapper `ClassName` if provided, a boolean indicating mapper presence, and the inferred target type; returns a disabled mapping (no domain, no mapper, mapper-present=false) if `domainType` is null or void
         */
    private TypeMapping extractTypeMapping(TypeMirror domainType, TypeMirror mapperTypeMirror) {
        if (domainType == null
                || domainType.getKind() == javax.lang.model.type.TypeKind.VOID
                || domainType.toString().equals("java.lang.Void")) {
            return new TypeMapping(null, null, false, null);
        }

        ClassName mapperType = resolveOptionalMapperType(mapperTypeMirror);

        return new TypeMapping(
            TypeName.get(domainType),
            mapperType,
            mapperType != null,
            TypeName.get(domainType)
        );
    }

    /**
     * Resolve an optional mapper TypeMirror to a ClassName.
     *
     * @param mapperTypeMirror the annotation TypeMirror for a mapper (may be null or represent void)
     * @return the resolved ClassName for the mapper, or `null` if the mirror is null, represents `void`/`java.lang.Void`, or cannot be resolved
     */
    private ClassName resolveOptionalMapperType(TypeMirror mapperTypeMirror) {
        return resolveClassNameFromMirror(mapperTypeMirror);
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

    /**
     * Determine the configured cache key generator class from the given annotation mirror.
     *
     * Reads the `cacheKeyGenerator` value and returns its ClassName unless the value is absent
     * or equals the default `io.quarkus.cache.CacheKeyGenerator`, in which case `null` is returned.
     *
     * @param annotationMirror the annotation mirror to read the `cacheKeyGenerator` value from
     * @return the ClassName of the configured cache key generator, or `null` if none or the default is used
     */
    private ClassName resolveCacheKeyGenerator(AnnotationMirror annotationMirror) {
        TypeMirror typeMirror = AnnotationProcessingUtils.getAnnotationValue(annotationMirror, "cacheKeyGenerator");
        if (typeMirror == null) {
            return null;
        }

        // Check if it's the default value (io.quarkus.cache.CacheKeyGenerator)
        TypeElement defaultElement = processingEnv.getElementUtils()
            .getTypeElement("io.quarkus.cache.CacheKeyGenerator");
        if (defaultElement != null && processingEnv.getTypeUtils().isSameType(typeMirror, defaultElement.asType())) {
            return null;
        }

        return resolveTypeClass(annotationMirror, "cacheKeyGenerator");
    }

    /**
     * Resolves a ClassName from an annotation value that specifies a type.
     * Handles void types, null values, and converts TypeMirror to ClassName.
     *
     * @param annotationMirror the annotation mirror to extract the value from
     * @param fieldName the name of the annotation value to extract
     * @return the ClassName for the specified type, or null if not specified or void
     */
    private ClassName resolveTypeClass(AnnotationMirror annotationMirror, String fieldName) {
        TypeMirror typeMirror = AnnotationProcessingUtils.getAnnotationValue(annotationMirror, fieldName);
        return resolveClassNameFromMirror(typeMirror);
    }

    /**
     * Resolve a TypeMirror into a ClassName representing the referenced type.
     *
     * @param typeMirror the type mirror to resolve; may be null or represent `void`/`java.lang.Void`
     * @return the ClassName for the referenced type, or `null` if the provided mirror is null or represents void/Void
     */
    private ClassName resolveClassNameFromMirror(TypeMirror typeMirror) {
        if (isNullOrVoid(typeMirror)) {
            return null;
        }
        Element element = processingEnv.getTypeUtils().asElement(typeMirror);
        if (element instanceof TypeElement typeElement) {
            return ClassName.get(typeElement);
        }
        return ClassName.bestGuess(typeMirror.toString());
    }

    /**
     * Determines whether a TypeMirror is null or represents the void type.
     *
     * @param typeMirror the type to check; may be null
     * @return `true` if the provided type is null, `void`, or `java.lang.Void`, `false` otherwise
     */
    private boolean isNullOrVoid(TypeMirror typeMirror) {
        return typeMirror == null
            || typeMirror.getKind() == javax.lang.model.type.TypeKind.VOID
            || typeMirror.toString().equals("java.lang.Void");
    }

    /**
     * Resolve the delegate service class referenced in the PipelineStep annotation.
     *
     * If both `operator` and `delegate` are present with different values, an error is reported
     * and the `operator` value is returned.
     *
     * @param annotationMirror the PipelineStep annotation mirror to read `operator` and `delegate` from
     * @return the ClassName for the resolved delegate (the `operator` if present, otherwise the `delegate`),
     *         or `null` if neither is specified
     */
    private ClassName resolveDelegateService(AnnotationMirror annotationMirror) {
        ClassName operator = resolveTypeClass(annotationMirror, "operator");
        ClassName delegate = resolveTypeClass(annotationMirror, "delegate");
        if (operator != null && delegate != null && !operator.equals(delegate)) {
            processingEnv.getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "@PipelineStep declares both operator() and delegate() with different values; use only one alias.");
            return operator;
        }
        return operator != null ? operator : delegate;
    }

    /**
     * Determine which mapper type is declared on the PipelineStep annotation.
     *
     * Prefers `operatorMapper` when present. If both `operatorMapper` and `externalMapper`
     * are specified with different values an error is reported and `operatorMapper` is returned.
     *
     * @param annotationMirror the PipelineStep annotation mirror to read mapper fields from
     * @return the resolved mapper `ClassName`, or `null` if neither mapper is specified
     */
    private ClassName resolveExternalMapper(AnnotationMirror annotationMirror) {
        ClassName operatorMapper = resolveTypeClass(annotationMirror, "operatorMapper");
        ClassName externalMapper = resolveTypeClass(annotationMirror, "externalMapper");
        if (operatorMapper != null && externalMapper != null && !operatorMapper.equals(externalMapper)) {
            processingEnv.getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "@PipelineStep declares both operatorMapper() and externalMapper() with different values; use only one alias.");
            return operatorMapper;
        }
        return operatorMapper != null ? operatorMapper : externalMapper;
    }
}