package org.pipelineframework.processor.renderer;

import java.io.IOException;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.RestBinding;
import org.pipelineframework.processor.util.DtoTypeUtils;
import org.pipelineframework.processor.util.ResourceNameUtils;
import org.pipelineframework.processor.util.RestPathResolver;

/**
 * Renderer for REST resource implementations based on PipelineStepModel and RestBinding
 */
public class RestResourceRenderer implements PipelineRenderer<RestBinding> {

    /**
     * Creates a new RestResourceRenderer.
     */
    public RestResourceRenderer() {
    }

    /**
     * Identify the generation target produced by this renderer.
     *
     * @return GenerationTarget.REST_RESOURCE when the renderer generates REST resource classes
     */
    @Override
    public GenerationTarget target() {
        return GenerationTarget.REST_RESOURCE;
    }

    /**
     * Generates and writes a REST resource class for the given binding into the generation context.
     *
     * @param binding the RestBinding describing the service, model and REST-specific overrides used to build the resource class
     * @param ctx the GenerationContext providing access to the output directory where the generated Java file will be written
     * @throws IOException if writing the generated Java file to the provided writer fails
     */
    @Override
    public void render(RestBinding binding, GenerationContext ctx) throws IOException {
        TypeSpec restResourceClass = buildRestResourceClass(binding, ctx);

        // Write the generated class
        JavaFile javaFile = JavaFile.builder(
            binding.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
            restResourceClass)
            .build();

        javaFile.writeTo(ctx.outputDir());
    }

    /**
     * Build a JAX-RS REST resource TypeSpec for the provided RestBinding and generation context.
     *
     * The generated class is a public REST resource annotated with @Path and a generated-role
     * annotation, contains an injected domain service and optional mapper fields, extends the
     * appropriate REST adapter, and includes DTO conversion and a process endpoint matching the
     * service's streaming shape.
     *
     * @param binding the RestBinding containing the PipelineStepModel, service name, optional path override, and mapping info
     * @param ctx the GenerationContext providing deployment role and output configuration
     * @return a TypeSpec representing the generated REST resource class
     */
    private TypeSpec buildRestResourceClass(RestBinding binding, GenerationContext ctx) {
        org.pipelineframework.processor.ir.DeploymentRole role = ctx.role();
        PipelineStepModel model = binding.model();
        boolean cachePluginSideEffect = isCachePluginSideEffect(model);
        boolean cacheSideEffect = isCacheSideEffect(model);
        if (!cacheSideEffect && !cachePluginSideEffect) {
            validateRestMappings(model);
        }

        String serviceClassName = model.generatedName();

        String baseName = ResourceNameUtils.normalizeBaseName(serviceClassName);
        String resourceClassName = baseName + PipelineStepProcessor.REST_RESOURCE_SUFFIX;

        // Create the REST resource class
        TypeSpec.Builder resourceBuilder = TypeSpec.classBuilder(resourceClassName)
            .addModifiers(Modifier.PUBLIC)
            // Add the GeneratedRole annotation to indicate this is a REST server
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.pipelineframework.annotation", "GeneratedRole"))
                    .addMember("value", "$T.$L",
                        ClassName.get("org.pipelineframework.annotation", "GeneratedRole", "Role"),
                        role.name())
                    .build());

        // Add @Path annotation - derive path from REST naming strategy or use provided path override
        String servicePath = binding.restPathOverride() != null
            ? binding.restPathOverride()
            : RestPathResolver.resolveResourcePath(model, ctx.processingEnv());
        resourceBuilder.addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Path"))
            .addMember("value", "$S", servicePath)
            .build());

        // Add service field with @Inject
        FieldSpec serviceField = FieldSpec.builder(
            resolveServiceType(model),
            "domainService")
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.inject", "Inject")).build())
            .build();
        resourceBuilder.addField(serviceField);
        // Add mapper fields with @Inject if they exist
        String inboundMapperFieldName = "inboundMapper";
        String outboundMapperFieldName = "outboundMapper";
        TypeName inboundMapperType = null;
        TypeName outboundMapperType = null;
        boolean inboundMapperAdded = false;

        if (!cacheSideEffect && model.inputMapping().hasMapper()) {
            inboundMapperType = model.inputMapping().mapperType();
            String inboundMapperSimpleName = inboundMapperType.toString().substring(
                inboundMapperType.toString().lastIndexOf('.') + 1);
            inboundMapperFieldName = inboundMapperSimpleName.substring(0, 1).toLowerCase() +
                inboundMapperSimpleName.substring(1);

            FieldSpec inboundMapperField = FieldSpec.builder(
                inboundMapperType,
                inboundMapperFieldName)
                .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.inject", "Inject")).build())
                .build();
            resourceBuilder.addField(inboundMapperField);
            inboundMapperAdded = true;
        }

        if (!cacheSideEffect && model.outputMapping().hasMapper()) {
            outboundMapperType = model.outputMapping().mapperType();
            String outboundMapperSimpleName = outboundMapperType.toString().substring(
                outboundMapperType.toString().lastIndexOf('.') + 1);
            outboundMapperFieldName = outboundMapperSimpleName.substring(0, 1).toLowerCase() +
                outboundMapperSimpleName.substring(1);

            boolean sameMapper = inboundMapperAdded && outboundMapperType.equals(inboundMapperType);
            if (sameMapper) {
                outboundMapperFieldName = inboundMapperFieldName;
            } else {
                if (inboundMapperAdded && outboundMapperFieldName.equals(inboundMapperFieldName)) {
                    outboundMapperFieldName = "outboundMapper";
                }
                FieldSpec outboundMapperField = FieldSpec.builder(
                    outboundMapperType,
                    outboundMapperFieldName)
                    .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.inject", "Inject")).build())
                    .build();
                resourceBuilder.addField(outboundMapperField);
            }
        }

        // For REST resources, we use appropriate DTO types, not gRPC types
        // The DTO types should be derived from domain types using the same
        // transformation logic as the original getDtoType method would have used
        TypeName inputDtoClassName = model.inboundDomainType() != null
            ? convertDomainToDtoType(model.inboundDomainType())
            : ClassName.OBJECT;
        TypeName outputDtoClassName = model.outboundDomainType() != null
            ? convertDomainToDtoType(model.outboundDomainType())
            : ClassName.OBJECT;

        TypeName domainInputType = cacheSideEffect
            ? inputDtoClassName
            : (model.inboundDomainType() != null ? model.inboundDomainType() : ClassName.OBJECT);
        TypeName domainOutputType = cacheSideEffect
            ? outputDtoClassName
            : (model.outboundDomainType() != null ? model.outboundDomainType() : ClassName.OBJECT);

        ClassName adapterClass = resolveRestAdapterClass(model);
        TypeName adapterType = ParameterizedTypeName.get(
            adapterClass,
            inputDtoClassName,
            outputDtoClassName,
            domainInputType,
            domainOutputType);
        resourceBuilder.superclass(adapterType);

        resourceBuilder.addMethod(createGetServiceMethod(model, domainInputType, domainOutputType));
        resourceBuilder.addMethod(createFromDtoMethod(model, inputDtoClassName, inboundMapperFieldName, cacheSideEffect));
        resourceBuilder.addMethod(createToDtoMethod(model, outputDtoClassName, outboundMapperFieldName, cacheSideEffect));

        String operationPath = RestPathResolver.resolveOperationPath(ctx.processingEnv());

        // Create the process method based on service type (determined from streaming shape)
        MethodSpec processMethod = switch (model.streamingShape()) {
            case UNARY_STREAMING -> createReactiveStreamingServiceProcessMethod(
                    inputDtoClassName, outputDtoClassName, model, inboundMapperFieldName, outboundMapperFieldName,
                    cachePluginSideEffect, operationPath);
            case STREAMING_UNARY -> createReactiveStreamingClientServiceProcessMethod(
                    inputDtoClassName, outputDtoClassName, model, inboundMapperFieldName, outboundMapperFieldName,
                    cachePluginSideEffect, operationPath);
            case STREAMING_STREAMING -> createReactiveBidirectionalStreamingServiceProcessMethod(
                    inputDtoClassName, outputDtoClassName, model, inboundMapperFieldName, outboundMapperFieldName,
                    cachePluginSideEffect, operationPath);
            default -> createReactiveServiceProcessMethod(
                    inputDtoClassName, outputDtoClassName, model, inboundMapperFieldName,
                    outboundMapperFieldName, cacheSideEffect, operationPath);
        };

        resourceBuilder.addMethod(processMethod);

        return resourceBuilder.build();
    }

    /**
         * Create the REST POST handler for the step's unary reactive "process" endpoint.
         *
         * The generated method is a public POST endpoint at the resolved operation path. When `cacheSideEffect`
         * is true the method forwards the incoming DTO directly to the domain service; when false
         * it converts the incoming DTO to a domain value using the inbound mapper and converts the
         * service result back to a DTO using the outbound mapper.
         *
         * @param inputDtoClassName       the DTO type used as the method parameter
         * @param outputDtoClassName      the DTO type produced by the method
         * @param model                   the pipeline step model that provides execution metadata and domain types
         * @param ctx                     the generation context carrying enabled aspects and deployment role
         * @param inboundMapperFieldName  name of the injected inbound mapper field used to convert DTO to domain (used when `cacheSideEffect` is false)
         * @param outboundMapperFieldName name of the injected outbound mapper field used to convert domain to DTO (used when `cacheSideEffect` is false)
         * @param cacheSideEffect         if true, bypass mapping and pass DTOs directly to/from the domain service
         * @return                        a MethodSpec representing a POST handler that returns a `Uni` of the output DTO
         */
    private MethodSpec createReactiveServiceProcessMethod(
            TypeName inputDtoClassName, TypeName outputDtoClassName,
            PipelineStepModel model,
            String inboundMapperFieldName,
            String outboundMapperFieldName,
            boolean cacheSideEffect,
            String operationPath) {
        if (!cacheSideEffect) {
            validateRestMappings(model);
        }

        TypeName uniOutputDto = ParameterizedTypeName.get(ClassName.get(Uni.class), outputDtoClassName);
        MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("process")
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "POST"))
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Path"))
                .addMember("value", "$S", operationPath)
                .build())
            .addModifiers(Modifier.PUBLIC)
            .returns(uniOutputDto)
            .addParameter(inputDtoClassName, "inputDto");

        if (cacheSideEffect) {
            methodBuilder.addStatement("return domainService.process(inputDto)");
        } else {
            TypeName domainInputType = model.inboundDomainType() != null
                ? model.inboundDomainType()
                : ClassName.OBJECT;
            methodBuilder.addStatement("$T inputDomain = $L.fromDto(inputDto)",
                domainInputType, inboundMapperFieldName);
            methodBuilder.addStatement("return domainService.process(inputDomain).map(output -> $L.toDto(output))",
                outboundMapperFieldName);
        }

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (model.executionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            methodBuilder.addAnnotation(ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        return methodBuilder.build();
    }

    /**
     * Exposes a POST endpoint at the resolved operation path that accepts a single input DTO and returns a JSON stream of output DTOs.
     *
     * @param model                   the pipeline step model that provides domain types and execution mode
     * @param inboundMapperFieldName  name of the injected inbound mapper used to convert the input DTO to the domain type
     * @param outboundMapperFieldName name of the injected outbound mapper used to convert domain outputs to DTOs
     * @param skipValidation          when true, skips validation of REST mappings before generating mapping calls
     * @return                        a `Multi` that emits output DTO instances produced from the service's domain outputs
     */
    private MethodSpec createReactiveStreamingServiceProcessMethod(
            TypeName inputDtoClassName, TypeName outputDtoClassName,
            PipelineStepModel model,
            String inboundMapperFieldName,
            String outboundMapperFieldName,
            boolean skipValidation,
            String operationPath) {
        if (!skipValidation) {
            validateRestMappings(model);
        }

        TypeName multiOutputDto = ParameterizedTypeName.get(ClassName.get(Multi.class), outputDtoClassName);

        MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("process")
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "POST"))
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Path"))
                .addMember("value", "$S", operationPath)
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.jboss.resteasy.reactive", "RestStreamElementType"))
                .addMember("value", "$S", "application/json")
                .build())
            .addModifiers(Modifier.PUBLIC)
            .returns(multiOutputDto)
            .addParameter(inputDtoClassName, "inputDto");

        TypeName domainInputType = model.inboundDomainType() != null
            ? model.inboundDomainType()
            : ClassName.OBJECT;
        methodBuilder.addStatement("$T inputDomain = $L.fromDto(inputDto)",
            domainInputType, inboundMapperFieldName);
        methodBuilder.addStatement("return domainService.process(inputDomain).map(output -> $L.toDto(output))",
            outboundMapperFieldName);

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (model.executionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            methodBuilder.addAnnotation(ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        return methodBuilder.build();
    }

    /**
     * Builds the reactive client-streaming JAX-RS "process" method for this REST resource.
     *
     * The generated method is a public POST endpoint at the resolved operation path that accepts a Multi of input DTOs,
     * maps each input DTO to a domain object using the inbound mapper, delegates processing to the domain service,
     * maps the domain result to an output DTO using the outbound mapper, and returns a Uni of output DTOs.
     * If skipValidation is false, REST mappings are validated before generation. If the model's execution mode is
     * VIRTUAL_THREADS, the method will be annotated to run on virtual threads.
     *
     * @param inputDtoClassName        the TypeName used for individual input DTOs
     * @param outputDtoClassName       the TypeName used for the output DTO
     * @param model                    the pipeline step model; influences validation and execution-mode annotation
     * @param inboundMapperFieldName   the injected inbound mapper field name used to convert DTOs to domain objects
     * @param outboundMapperFieldName  the injected outbound mapper field name used to convert domain objects to DTOs
     * @param skipValidation           if true, skips validateRestMappings(model) during generation
     * @return                         a MethodSpec representing the generated "process" method
     */
    private MethodSpec createReactiveStreamingClientServiceProcessMethod(
            TypeName inputDtoClassName, TypeName outputDtoClassName,
            PipelineStepModel model,
            String inboundMapperFieldName,
            String outboundMapperFieldName,
            boolean skipValidation,
            String operationPath) {
        if (!skipValidation) {
            validateRestMappings(model);
        }

        TypeName listInputDto = ParameterizedTypeName.get(ClassName.get(java.util.List.class), inputDtoClassName);
        TypeName uniOutputDto = ParameterizedTypeName.get(ClassName.get(Uni.class), outputDtoClassName);

        MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("process")
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "POST"))
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Path"))
                .addMember("value", "$S", operationPath)
                .build())
            .addModifiers(Modifier.PUBLIC)
            .returns(uniOutputDto)
            .addParameter(ParameterSpec.builder(listInputDto, "inputDtos")
                .build());

        methodBuilder.addStatement(
            "return domainService.process($T.createFrom().iterable(inputDtos).map(item -> $L.fromDto(item))).map(output -> $L.toDto(output))",
            ClassName.get(Multi.class),
            inboundMapperFieldName,
            outboundMapperFieldName);

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (model.executionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            methodBuilder.addAnnotation(ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        return methodBuilder.build();
    }

    /**
     * Exposes a bidirectional streaming POST endpoint at the resolved operation path that accepts a stream of input DTOs and returns a stream of output DTOs.
     *
     * @param inputDtoClassName       the DTO type of incoming stream elements
     * @param outputDtoClassName      the DTO type of outgoing stream elements
     * @param model                   pipeline step model; if its execution mode is VIRTUAL_THREADS the generated method is annotated with `@RunOnVirtualThread`
     * @param inboundMapperFieldName  name of the injected mapper field used to convert incoming DTOs to domain objects
     * @param outboundMapperFieldName name of the injected mapper field used to convert domain outputs to DTOs
     * @param skipValidation          if true, REST mapping validation is skipped for this method
     * @return                        a `Multi` of output DTO instances produced from the domain service's outputs
     */
    private MethodSpec createReactiveBidirectionalStreamingServiceProcessMethod(
            TypeName inputDtoClassName, TypeName outputDtoClassName,
            PipelineStepModel model,
            String inboundMapperFieldName,
            String outboundMapperFieldName,
            boolean skipValidation,
            String operationPath) {
        if (!skipValidation) {
            validateRestMappings(model);
        }

        TypeName multiInputDto = ParameterizedTypeName.get(ClassName.get(Multi.class), inputDtoClassName);
        TypeName multiOutputDto = ParameterizedTypeName.get(ClassName.get(Multi.class), outputDtoClassName);

        MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("process")
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "POST"))
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Path"))
                .addMember("value", "$S", operationPath)
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Consumes"))
                .addMember("value", "$S",  "application/x-ndjson")
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Produces"))
                .addMember("value", "$S",  "application/x-ndjson")
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.jboss.resteasy.reactive", "RestStreamElementType"))
                .addMember("value", "$S", "application/json")
                .build())
            .addModifiers(Modifier.PUBLIC)
            .returns(multiOutputDto)
            .addParameter(multiInputDto, "inputDtos");

        methodBuilder.addStatement(
            "return domainService.process(inputDtos.map(item -> $L.fromDto(item))).map(output -> $L.toDto(output))",
            inboundMapperFieldName,
            outboundMapperFieldName);

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (model.executionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            methodBuilder.addAnnotation(ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        return methodBuilder.build();
    }

    /**
     * Selects the REST reactive adapter class corresponding to the model's streaming shape.
     *
     * @param model the pipeline step model whose streaming shape determines the adapter
     * @return the ClassName of the matching REST reactive adapter
     */
    private ClassName resolveRestAdapterClass(PipelineStepModel model) {
        return switch (model.streamingShape()) {
            case UNARY_STREAMING -> ClassName.get(
                "org.pipelineframework.rest", "RestReactiveStreamingServiceAdapter");
            case STREAMING_UNARY -> ClassName.get(
                "org.pipelineframework.rest", "RestReactiveStreamingClientServiceAdapter");
            case STREAMING_STREAMING -> ClassName.get(
                "org.pipelineframework.rest", "RestReactiveBidirectionalStreamingServiceAdapter");
            default -> ClassName.get(
                "org.pipelineframework.rest", "RestReactiveServiceAdapter");
        };
    }

    /**
     * Builds the protected `getService()` method that returns the appropriately parameterized reactive service type for the pipeline step.
     *
     * @param model the pipeline step model whose streaming shape determines the specific reactive service interface
     * @param domainInputType the domain-level input type used to parameterize the returned service
     * @param domainOutputType the domain-level output type used to parameterize the returned service
     * @return the service instance typed to the reactive service interface matching the step's streaming shape and parameterized with the provided domain input and output types
     */
    private MethodSpec createGetServiceMethod(
            PipelineStepModel model,
            TypeName domainInputType,
            TypeName domainOutputType) {
        TypeName serviceType = switch (model.streamingShape()) {
            case UNARY_STREAMING -> ParameterizedTypeName.get(
                ClassName.get("org.pipelineframework.service", "ReactiveStreamingService"),
                domainInputType,
                domainOutputType);
            case STREAMING_UNARY -> ParameterizedTypeName.get(
                ClassName.get("org.pipelineframework.service", "ReactiveStreamingClientService"),
                domainInputType,
                domainOutputType);
            case STREAMING_STREAMING -> ParameterizedTypeName.get(
                ClassName.get("org.pipelineframework.service", "ReactiveBidirectionalStreamingService"),
                domainInputType,
                domainOutputType);
            default -> ParameterizedTypeName.get(
                ClassName.get("org.pipelineframework.service", "ReactiveService"),
                domainInputType,
                domainOutputType);
        };

        return MethodSpec.methodBuilder("getService")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED)
            .returns(serviceType)
            .addStatement("return domainService")
            .build();
    }

    /**
     * Determine which service class should be injected for the pipeline step.
     *
     * @param model the pipeline step model used to resolve the service type
     * @return the service class to use: the model's declared service class when the step is not a side effect, otherwise the service class located in the pipeline package derived from the model's service package and service name
     */
    private TypeName resolveServiceType(PipelineStepModel model) {
        if (!model.sideEffect()) {
            return model.serviceClassName();
        }
        return ClassName.get(
            model.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
            model.serviceName());
    }

    /**
     * Determines whether the given pipeline step model represents a cache-based side effect
     * that uses the framework CacheService and expects a unary-to-unary streaming shape.
     *
     * @param model the pipeline step model to inspect
     * @return `true` if the model is a side effect backed by `org.pipelineframework.plugin.cache.CacheService`
     *         and its streaming shape is `UNARY_UNARY`, `false` otherwise
     */
    private boolean isCacheSideEffect(PipelineStepModel model) {
        if (model == null || !model.sideEffect() || model.serviceClassName() == null) {
            return false;
        }
        return model.streamingShape() == org.pipelineframework.processor.ir.StreamingShape.UNARY_UNARY
            && "org.pipelineframework.plugin.cache.CacheService".equals(
                model.serviceClassName().canonicalName());
    }

    /**
     * Determines whether the given pipeline step model represents a cache plugin side-effect.
     *
     * @param model the pipeline step model to inspect; may be null
     * @return `true` if the model is a side-effect and its service class is
     *         `org.pipelineframework.plugin.cache.CacheService`, `false` otherwise
     */
    private boolean isCachePluginSideEffect(PipelineStepModel model) {
        if (model == null || !model.sideEffect() || model.serviceClassName() == null) {
            return false;
        }
        return "org.pipelineframework.plugin.cache.CacheService".equals(
            model.serviceClassName().canonicalName());
    }

    /**
     * Converts an input DTO into the step's domain input value.
     *
     * If `cacheSideEffect` is true, the DTO is returned unchanged; otherwise the inbound mapper
     * referenced by `inboundMapperFieldName` is used to perform the conversion.
     *
     * @param model the pipeline step model providing domain type metadata
     * @param inputDtoClassName the compile-time type of the incoming DTO parameter
     * @param inboundMapperFieldName the name of the injected inbound mapper field to call when mapping
     * @param cacheSideEffect when true, skip mapping and pass the DTO through as the domain input
     * @return the domain input value derived from `dto`, or the original `dto` when `cacheSideEffect` is true
     */
    private MethodSpec createFromDtoMethod(
            PipelineStepModel model,
            TypeName inputDtoClassName,
            String inboundMapperFieldName,
            boolean cacheSideEffect) {
        TypeName domainInputType = cacheSideEffect
            ? inputDtoClassName
            : (model.inboundDomainType() != null ? model.inboundDomainType() : ClassName.OBJECT);
        MethodSpec.Builder builder = MethodSpec.methodBuilder("fromDto")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED)
            .returns(domainInputType)
            .addParameter(inputDtoClassName, "dto");
        if (cacheSideEffect) {
            builder.addStatement("return dto");
        } else {
            builder.addStatement("return $L.fromDto(dto)", inboundMapperFieldName);
        }
        return builder.build();
    }

    /**
     * Convert a domain object into its outbound DTO.
     *
     * @param model the pipeline step metadata used to determine domain/outbound types
     * @param outputDtoClassName the DTO type returned by this method
     * @param outboundMapperFieldName the name of the injected outbound mapper field to use when mapping is required
     * @param cacheSideEffect if `true`, the method returns the input value unchanged; if `false`, the method delegates to the outbound mapper
     * @return the outbound DTO corresponding to the given domain object
     */
    private MethodSpec createToDtoMethod(
            PipelineStepModel model,
            TypeName outputDtoClassName,
            String outboundMapperFieldName,
            boolean cacheSideEffect) {
        TypeName domainOutputType = cacheSideEffect
            ? outputDtoClassName
            : (model.outboundDomainType() != null ? model.outboundDomainType() : ClassName.OBJECT);
        MethodSpec.Builder builder = MethodSpec.methodBuilder("toDto")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PROTECTED)
            .returns(outputDtoClassName)
            .addParameter(domainOutputType, "domain");
        if (cacheSideEffect) {
            builder.addStatement("return domain");
        } else {
            builder.addStatement("return $L.toDto(domain)", outboundMapperFieldName);
        }
        return builder.build();
    }

    /**
     * Derives the DTO TypeName for a given domain TypeName.
     *
     * <p>If {@code domainType} is {@code null} the method returns {@code ClassName.OBJECT}. Otherwise
     * it converts common package segments (for example {@code .domain.} or {@code .service.}) to
     * {@code .dto.} and appends the suffix {@code Dto} to the simple class name to produce the DTO
     * TypeName.
     *
     * @param domainType the domain type to convert; may be {@code null}
     * @return the corresponding DTO TypeName, or {@code ClassName.OBJECT} when {@code domainType} is {@code null}
     */
    private TypeName convertDomainToDtoType(TypeName domainType) {
        return DtoTypeUtils.toDtoType(domainType);
    }

    /**
     * Validate that the pipeline step model contains required input/output mapping configuration for REST generation.
     *
     * @param model the pipeline step model to validate
     * @throws IllegalStateException if {@code model} is null, if input or output mappings are missing, or if either mapping lacks a resolved mapper or a non-null domain type
     */
    private void validateRestMappings(PipelineStepModel model) {
        if (model == null) {
            throw new IllegalStateException("REST resource generation requires a non-null PipelineStepModel");
        }
        if (model.inputMapping() == null || model.outputMapping() == null) {
            throw new IllegalStateException(String.format(
                "REST resource generation for '%s' requires input/output mappings to be present",
                model.serviceName()));
        }
        if (!model.inputMapping().hasMapper() || model.inputMapping().domainType() == null) {
            throw new IllegalStateException(String.format(
                "REST resource generation for '%s' requires a non-null input domain type and mapper; " +
                "all mappers must be resolved at build time (inputMapping=%s).",
                model.serviceName(),
                model.inputMapping()));
        }
        if (!model.outputMapping().hasMapper() || model.outputMapping().domainType() == null) {
            throw new IllegalStateException(String.format(
                "REST resource generation for '%s' requires a non-null output domain type and mapper; " +
                "all mappers must be resolved at build time (outputMapping=%s).",
                model.serviceName(),
                model.outputMapping()));
        }
    }

}