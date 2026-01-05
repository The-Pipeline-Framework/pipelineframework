package org.pipelineframework.processor.renderer;

import java.io.IOException;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.RestBinding;

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
     * Builds a REST resource class TypeSpec from the given binding.
     *
     * Constructs a public REST resource class populated with JAX-RS and DI annotations,
     * injected service and optional mapper fields, a logger, a process endpoint tailored
     * to the service's streaming shape and DTO conversions, and an exception mapper.
     *
     * @param binding the RestBinding that provides the PipelineStepModel, service name,
     *                optional path override and mapping information used to generate the resource
     * @return the generated TypeSpec representing the REST resource class
     */
    private TypeSpec buildRestResourceClass(RestBinding binding, GenerationContext ctx) {
        org.pipelineframework.processor.ir.DeploymentRole role = ctx.role();
        PipelineStepModel model = binding.model();
        validateRestMappings(model);

        String serviceClassName = model.generatedName();

        // Determine the resource class name - remove "Service" and optionally "Reactive" for cleaner naming
        String baseName = serviceClassName.replace("Service", "");
        if (baseName.endsWith("Reactive")) {
            baseName = baseName.substring(0, baseName.length() - "Reactive".length());
        }
        String resourceClassName = baseName + PipelineStepProcessor.REST_RESOURCE_SUFFIX;

        // Create the REST resource class
        TypeSpec.Builder resourceBuilder = TypeSpec.classBuilder(resourceClassName)
            .addModifiers(Modifier.PUBLIC)
            // Add the GeneratedRole annotation to indicate this is a REST server
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.pipelineframework.annotation", "GeneratedRole"))
                    .addMember("value", "$T.$L",
                        ClassName.get("org.pipelineframework.annotation.GeneratedRole", "Role"),
                        role.name())
                    .build());

        // Add @Path annotation - derive path from service class name or use provided path
        String servicePath = binding.restPathOverride() != null ? binding.restPathOverride() : deriveResourcePath(serviceClassName);
        resourceBuilder.addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Path"))
            .addMember("value", "$S", servicePath)
            .build());

        // Add service field with @Inject
        FieldSpec serviceField = FieldSpec.builder(
            model.serviceClassName(),
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

        if (model.inputMapping().hasMapper()) {
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

        if (model.outputMapping().hasMapper()) {
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

        // Add logger field to the resource class
        FieldSpec loggerField = FieldSpec.builder(
            ClassName.get("org.jboss.logging", "Logger"),
            "logger")
            .addModifiers(Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .initializer("$T.getLogger($L.class)",
                ClassName.get("org.jboss.logging", "Logger"),
                resourceClassName)
            .build();
        resourceBuilder.addField(loggerField);

        // For REST resources, we use appropriate DTO types, not gRPC types
        // The DTO types should be derived from domain types using the same
        // transformation logic as the original getDtoType method would have used
        TypeName inputDtoClassName = model.inboundDomainType() != null ?
            convertDomainToDtoType(model.inboundDomainType()) : ClassName.OBJECT;
        TypeName outputDtoClassName = model.outboundDomainType() != null ?
            convertDomainToDtoType(model.outboundDomainType()) : ClassName.OBJECT;

        // Create the process method based on service type (determined from streaming shape)
        MethodSpec processMethod = switch (model.streamingShape()) {
            case UNARY_STREAMING -> createReactiveStreamingServiceProcessMethod(
                    inputDtoClassName, outputDtoClassName,
                    inboundMapperFieldName, outboundMapperFieldName, model);
            case STREAMING_UNARY -> createReactiveStreamingClientServiceProcessMethod(
                    inputDtoClassName, outputDtoClassName,
                    inboundMapperFieldName, outboundMapperFieldName, model);
            case STREAMING_STREAMING -> createReactiveBidirectionalStreamingServiceProcessMethod(
                    inputDtoClassName, outputDtoClassName,
                    inboundMapperFieldName, outboundMapperFieldName, model);
            default -> createReactiveServiceProcessMethod(
                    inputDtoClassName, outputDtoClassName,
                    inboundMapperFieldName, outboundMapperFieldName, model, ctx);
        };

        resourceBuilder.addMethod(processMethod);

        // Add exception mapper method to handle different types of exceptions
        MethodSpec exceptionMapperMethod = createExceptionMapperMethod();
        resourceBuilder.addMethod(exceptionMapperMethod);

        return resourceBuilder.build();
    }

    /**
     * Create the REST POST "process" method for a unary reactive service endpoint.
     *
     * The generated method is a public POST handler at path "/process" that accepts an input DTO,
     * converts it to the inbound domain type using the inbound mapper, delegates to the injected
     * domain service, and maps the service result to an output DTO using the outbound mapper.
     *
     * @param inputDtoClassName           the DTO type used as the method parameter
     * @param outputDtoClassName          the DTO type produced by the method
     * @param inboundMapperFieldName      name of the injected inbound mapper field used to convert DTO to domain
     * @param outboundMapperFieldName     name of the injected outbound mapper field used to convert domain to DTO
     * @param model                       the pipeline step model used to determine domain types and execution mode
     * @param ctx                         generation context carrying enabled aspects and deployment role
     * @return                            a MethodSpec for the generated REST "process" method that returns a `Uni` of the output DTO
     */
    private MethodSpec createReactiveServiceProcessMethod(
            TypeName inputDtoClassName, TypeName outputDtoClassName,
            String inboundMapperFieldName, String outboundMapperFieldName,
            PipelineStepModel model,
            GenerationContext ctx) {
        validateRestMappings(model);

        TypeName uniOutputDto = ParameterizedTypeName.get(ClassName.get(Uni.class), outputDtoClassName);

        MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("process")
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "POST"))
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Path"))
                .addMember("value", "$S", "/process")
                .build())
            .addModifiers(Modifier.PUBLIC)
            .returns(uniOutputDto)
            .addParameter(inputDtoClassName, "inputDto");

        // Add the implementation code
        methodBuilder.addStatement("$T inputDomain = $L.fromDto(inputDto)",
                model.inboundDomainType(),
                inboundMapperFieldName);

        methodBuilder.addStatement("return domainService.process(inputDomain).map(output -> $L.toDto(output))",
                outboundMapperFieldName);

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (model.executionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            methodBuilder.addAnnotation(ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        return maybeAddCacheAnnotation(methodBuilder.build(), model, ctx);
    }

    /**
     * Exposes a POST /process endpoint that accepts a single input DTO and returns a JSON stream of output DTOs.
     *
     * @param inputDtoClassName           the DTO type accepted by the endpoint
     * @param outputDtoClassName          the DTO type emitted by the returned stream
     * @param inboundMapperFieldName      name of the injected inbound mapper field used to convert the input DTO to the domain type
     * @param outboundMapperFieldName     name of the injected outbound mapper field used to convert domain outputs to DTOs
     * @param model                       pipeline step model used to determine domain types and execution mode
     * @return                            a `Multi` that emits output DTO instances converted from the service's domain outputs
     */
    private MethodSpec createReactiveStreamingServiceProcessMethod(
            TypeName inputDtoClassName, TypeName outputDtoClassName,
            String inboundMapperFieldName, String outboundMapperFieldName,
            PipelineStepModel model) {
        validateRestMappings(model);

        TypeName multiOutputDto = ParameterizedTypeName.get(ClassName.get(Multi.class), outputDtoClassName);

        MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("process")
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "POST"))
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Path"))
                .addMember("value", "$S", "/process")
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.jboss.resteasy.reactive", "RestStreamElementType"))
                .addMember("value", "$S", "application/json")
                .build())
            .addModifiers(Modifier.PUBLIC)
            .returns(multiOutputDto)
            .addParameter(inputDtoClassName, "inputDto");

        // Add the implementation code
        methodBuilder.addStatement("$T inputDomain = $L.fromDto(inputDto)",
                model.inboundDomainType(),
                inboundMapperFieldName);

        // Return the stream, allowing errors to propagate to the exception mapper
        methodBuilder.addStatement("return domainService.process(inputDomain).map(output -> $L.toDto(output))",
                outboundMapperFieldName);

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (model.executionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            methodBuilder.addAnnotation(ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        return methodBuilder.build();
    }

    /**
     * Builds the reactive client-streaming "process" JAX-RS method for the generated REST resource.
     *
     * The generated method is a public POST endpoint at path "/process" that accepts a Multi of input DTOs,
     * maps each DTO to its domain representation using the inbound mapper field, delegates processing to the
     * injected domain service, then maps the service result to an output DTO using the outbound mapper field.
     * If the pipeline step's execution mode is `VIRTUAL_THREADS`, the method will be annotated to run on a virtual thread.
     *
     * @param inputDtoClassName    the TypeName used for individual input DTOs
     * @param outputDtoClassName   the TypeName used for the output DTO
     * @param inboundMapperFieldName  the name of the injected inbound mapper field used to convert DTOs to domain objects
     * @param outboundMapperFieldName the name of the injected outbound mapper field used to convert domain objects to DTOs
     * @param model                the pipeline step model that influences method generation (for example execution mode)
     * @return                     a MethodSpec representing the generated "process" method
     */
    private MethodSpec createReactiveStreamingClientServiceProcessMethod(
            TypeName inputDtoClassName, TypeName outputDtoClassName,
            String inboundMapperFieldName, String outboundMapperFieldName,
            PipelineStepModel model) {
        validateRestMappings(model);

        TypeName multiInputDto = ParameterizedTypeName.get(ClassName.get(Multi.class), inputDtoClassName);
        TypeName uniOutputDto = ParameterizedTypeName.get(ClassName.get(Uni.class), outputDtoClassName);

        MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("process")
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "POST"))
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Path"))
                .addMember("value", "$S", "/process")
                .build())
            .addModifiers(Modifier.PUBLIC)
            .returns(uniOutputDto)
            .addParameter(ParameterSpec.builder(multiInputDto, "inputDtos")
                .build());

        // Add the implementation code
        methodBuilder.addStatement("$T<$T> domainInputs = inputDtos.map(input -> $L.fromDto(input))",
                ClassName.get(Multi.class),
                model.inboundDomainType(),
                inboundMapperFieldName);

        methodBuilder.addStatement("return domainService.process(domainInputs).map(output -> $L.toDto(output))",
                outboundMapperFieldName);

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (model.executionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            methodBuilder.addAnnotation(ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        return methodBuilder.build();
    }

    /**
     * Exposes a bidirectional streaming POST endpoint at "/process" that accepts a stream of input DTOs and returns a stream of output DTOs.
     *
     * @param inputDtoClassName      the DTO type for incoming stream elements
     * @param outputDtoClassName     the DTO type for outgoing stream elements
     * @param inboundMapperFieldName name of the injected mapper field used to convert incoming DTOs to domain objects
     * @param outboundMapperFieldName name of the injected mapper field used to convert domain outputs to DTOs
     * @param model                  the pipeline step model that influences streaming shape and execution mode; if the execution mode is VIRTUAL_THREADS the generated method will be annotated with `@RunOnVirtualThread`
     * @return                       a `Multi` of output DTO instances produced by mapping the domain service's outputs to DTOs
     */
    private MethodSpec createReactiveBidirectionalStreamingServiceProcessMethod(
            TypeName inputDtoClassName, TypeName outputDtoClassName,
            String inboundMapperFieldName, String outboundMapperFieldName,
            PipelineStepModel model) {
        validateRestMappings(model);

        TypeName multiInputDto = ParameterizedTypeName.get(ClassName.get(Multi.class), inputDtoClassName);
        TypeName multiOutputDto = ParameterizedTypeName.get(ClassName.get(Multi.class), outputDtoClassName);

        MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("process")
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "POST"))
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Path"))
                .addMember("value", "$S", "/process")
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

        // Add the implementation code
        methodBuilder.addStatement("$T<$T> domainInputs = inputDtos.map(input -> $L.fromDto(input))",
                ClassName.get(Multi.class),
                model.inboundDomainType(),
                inboundMapperFieldName);

        methodBuilder.addStatement("return domainService.process(domainInputs).map(output -> $L.toDto(output))",
                outboundMapperFieldName);

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (model.executionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            methodBuilder.addAnnotation(ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        return methodBuilder.build();
    }

    /**
     * Map an exception thrown by a resource method to an appropriate RestResponse.
     *
     * @return a RestResponse with `BAD_REQUEST` and message "Invalid request" when `ex` is an `IllegalArgumentException`; otherwise a RestResponse with `INTERNAL_SERVER_ERROR` and message "An unexpected error occurred"
     */
    private MethodSpec createExceptionMapperMethod() {
        TypeName responseType = ParameterizedTypeName.get(
            ClassName.get(RestResponse.class),
            ClassName.get(String.class));
        return MethodSpec.methodBuilder("handleException")
            .addAnnotation(AnnotationSpec.builder(ClassName.get(SuppressWarnings.class))
                .addMember("value", "$S", "unused")
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get(ServerExceptionMapper.class))
                .build())
            .addModifiers(Modifier.PUBLIC)
            .returns(responseType)
            .addParameter(Exception.class, "ex")
            .beginControlFlow("if (ex instanceof $T)", IllegalArgumentException.class)
                .addStatement("logger.warn(\"Invalid request\", ex)")
                .addStatement("return $T.status($T.Status.BAD_REQUEST, \"Invalid request\")",
                    ClassName.get(RestResponse.class),
                    ClassName.get("jakarta.ws.rs.core", "Response"))
            .nextControlFlow("else if (ex instanceof $T)", RuntimeException.class)
                .addStatement("logger.error(\"Unexpected error processing request\", ex)")
                .addStatement("return $T.status($T.Status.INTERNAL_SERVER_ERROR, \"An unexpected error occurred\")",
                    ClassName.get(RestResponse.class),
                    ClassName.get("jakarta.ws.rs.core", "Response"))
            .nextControlFlow("else")
                .addStatement("logger.error(\"Unexpected error processing request\", ex)")
                .addStatement("return $T.status($T.Status.INTERNAL_SERVER_ERROR, \"An unexpected error occurred\")",
                    ClassName.get(RestResponse.class),
                    ClassName.get("jakarta.ws.rs.core", "Response"))
            .endControlFlow()
            .build();
    }

    /**
     * Derives the REST resource path for a given service class name.
     *
     * The method removes a trailing "Service" suffix and any "Reactive" substring,
     * converts the resulting PascalCase name to kebab-case, and prefixes it with
     * "/api/v1/".
     *
     * @param className the original service class name (for example "ProcessPaymentReactiveService")
     * @return the resource path beginning with "/api/v1/" followed by the kebab-case name
     */
    private String deriveResourcePath(String className) {
        // Remove "Service" suffix if present
        if (className.endsWith("Service")) {
            className = className.substring(0, className.length() - 7);
        }

        // Remove "Reactive" if present (for service names like "ProcessPaymentReactiveService")
        className = className.replace("Reactive", "");

        // Convert from PascalCase to kebab-case
        // Handle sequences like "ProcessPaymentStatus" -> "process-payment-status"
        String pathPart = className.replaceAll("([a-z])([A-Z])", "$1-$2")
            .replaceAll("([A-Z]+)([A-Z][a-z])", "$1-$2")
            .toLowerCase();

        return "/api/v1/" + pathPart;
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
        if (domainType == null) {
            return ClassName.OBJECT;
        }

        String domainTypeStr = domainType.toString();

        // Replace common domain package patterns with DTO equivalents and add Dto suffix
        String dtoTypeStr = domainTypeStr
            .replace(".domain.", ".dto.")
            .replace(".service.", ".dto.");

        // If domain-to-dto package conversion succeeded, add Dto suffix to the class name
        if (!dtoTypeStr.equals(domainTypeStr)) {
            int lastDot = dtoTypeStr.lastIndexOf('.');
            String packageName = lastDot > 0 ? dtoTypeStr.substring(0, lastDot) : "";
            String simpleName = lastDot > 0 ? dtoTypeStr.substring(lastDot + 1) : dtoTypeStr;
            // Add Dto suffix to the class name
            String dtoSimpleName = simpleName + "Dto";
            return ClassName.get(packageName, dtoSimpleName);
        } else {
            // If domain/dto conversion didn't work (no standard domain package),
            // just add Dto suffix to the simple name
            int lastDot = domainTypeStr.lastIndexOf('.');
            String packageName = lastDot > 0 ? domainTypeStr.substring(0, lastDot) : "";
            String simpleName = lastDot > 0 ? domainTypeStr.substring(lastDot + 1) : domainTypeStr;
            // Add Dto suffix to the class name
            String dtoSimpleName = simpleName + "Dto";
            return ClassName.get(packageName, dtoSimpleName);
        }
    }

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
                "REST resource generation for '%s' requires a non-null input domain type and inbound mapper",
                model.serviceName()));
        }
        if (!model.outputMapping().hasMapper() || model.outputMapping().domainType() == null) {
            throw new IllegalStateException(String.format(
                "REST resource generation for '%s' requires a non-null output domain type and outbound mapper",
                model.serviceName()));
        }
    }

    private MethodSpec maybeAddCacheAnnotation(MethodSpec method, PipelineStepModel model, GenerationContext ctx) {
        if (!ctx.enabledAspects().contains("cache")) {
            return method;
        }
        if (ctx.role() != org.pipelineframework.processor.ir.DeploymentRole.REST_SERVER) {
            return method;
        }
        if (model.sideEffect() || model.streamingShape() != org.pipelineframework.processor.ir.StreamingShape.UNARY_UNARY) {
            return method;
        }

        AnnotationSpec cacheAnnotation = AnnotationSpec.builder(ClassName.get("io.quarkus.cache", "CacheResult"))
            .addMember("cacheName", "$S", "pipeline-cache")
            .addMember("keyGenerator", "$T.class", resolveCacheKeyGenerator(ctx))
            .build();

        return method.toBuilder()
            .addAnnotation(cacheAnnotation)
            .build();
    }

    private TypeName resolveCacheKeyGenerator(GenerationContext ctx) {
        if (ctx.cacheKeyGenerator() != null) {
            return ctx.cacheKeyGenerator();
        }
        return ClassName.get("org.pipelineframework.cache", "PipelineCacheKeyGenerator");
    }
}
