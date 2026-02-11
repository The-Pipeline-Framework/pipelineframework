package org.pipelineframework.processor.renderer;

import java.io.IOException;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;
import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.RestBinding;
import org.pipelineframework.processor.util.DtoTypeUtils;
import org.pipelineframework.processor.util.ResourceNameUtils;
import org.pipelineframework.step.StepManyToOne;
import org.pipelineframework.step.StepOneToOne;

/**
 * Renderer for REST client step implementations based on PipelineStepModel and RestBinding.
 */
public class RestClientStepRenderer implements PipelineRenderer<RestBinding> {

    /**
     * Creates a new RestClientStepRenderer.
     */
    public RestClientStepRenderer() {
    }

    @Override
    public GenerationTarget target() {
        return GenerationTarget.REST_CLIENT_STEP;
    }

    @Override
    public void render(RestBinding binding, GenerationContext ctx) throws IOException {
        TypeSpec restClientInterface = buildRestClientInterface(binding);
        TypeSpec restClientStep = buildRestClientStepClass(binding, ctx, restClientInterface.name);

        JavaFile clientInterfaceFile = JavaFile.builder(
                binding.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
                restClientInterface)
            .build();

        JavaFile clientStepFile = JavaFile.builder(
                binding.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
                restClientStep)
            .build();

        clientInterfaceFile.writeTo(ctx.outputDir());
        clientStepFile.writeTo(ctx.outputDir());
    }

    /**
     * Builds a JavaPoet TypeSpec for a REST client interface corresponding to the given RestBinding.
     *
     * The generated interface is public, annotated with REST client registration, client header factory and a cache-status
     * response filter, and is rooted at the binding's REST path. It declares a `process` method whose signature matches
     * the pipeline step's streaming shape and DTO types.
     *
     * @param binding the RestBinding that provides the pipeline model, DTO type information, and optional path override
     * @return a TypeSpec representing the generated REST client interface
     */
    private TypeSpec buildRestClientInterface(RestBinding binding) {
        PipelineStepModel model = binding.model();
        validateRestMappings(model);

        String serviceClassName = model.generatedName();
        String baseName = ResourceNameUtils.normalizeBaseName(serviceClassName);
        String interfaceName = baseName + "RestClient";

        TypeName inputDto = model.inboundDomainType() != null
            ? convertDomainToDtoType(model.inboundDomainType())
            : ClassName.OBJECT;
        TypeName outputDto = model.outboundDomainType() != null
            ? convertDomainToDtoType(model.outboundDomainType())
            : ClassName.OBJECT;

        String basePath = binding.restPathOverride() != null
            ? binding.restPathOverride()
            : deriveResourcePath(serviceClassName);

        AnnotationSpec registerRestClient = AnnotationSpec.builder(
                ClassName.get("org.eclipse.microprofile.rest.client.inject", "RegisterRestClient"))
            .addMember("configKey", "$S", toRestClientName(model.serviceName()))
            .build();
        AnnotationSpec registerClientHeaders = AnnotationSpec.builder(
                ClassName.get("org.eclipse.microprofile.rest.client.annotation", "RegisterClientHeaders"))
            .addMember("value", "$T.class",
                ClassName.get("org.pipelineframework.context.rest", "PipelineContextClientHeadersFactory"))
            .build();
        AnnotationSpec registerCacheStatusFilter = AnnotationSpec.builder(
                ClassName.get("org.eclipse.microprofile.rest.client.annotation", "RegisterProvider"))
            .addMember("value", "$T.class",
                ClassName.get("org.pipelineframework.context.rest", "PipelineCacheStatusClientResponseFilter"))
            .build();

        TypeSpec.Builder interfaceBuilder = TypeSpec.interfaceBuilder(interfaceName)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(registerRestClient)
            .addAnnotation(registerClientHeaders)
            .addAnnotation(registerCacheStatusFilter)
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Path"))
                .addMember("value", "$S", basePath)
                .build());

        MethodSpec processMethod = switch (model.streamingShape()) {
            case UNARY_STREAMING -> buildUnaryStreamingMethod(inputDto, outputDto);
            case STREAMING_UNARY -> buildStreamingUnaryMethod(inputDto, outputDto);
            case STREAMING_STREAMING -> buildStreamingStreamingMethod(inputDto, outputDto);
            default -> buildUnaryUnaryMethod(inputDto, outputDto);
        };

        interfaceBuilder.addMethod(processMethod);
        return interfaceBuilder.build();
    }

    /**
     * Builds a TypeSpec for a REST client step class corresponding to the provided binding.
     *
     * The generated class is a public ConfigurableStep subclass that wires an injected REST client,
     * applies parallelism and role annotations, conditionally implements cache-related interfaces,
     * and exposes step entry points that forward PipelineContext header values (versionTag, replayMode,
     * cachePolicy) to the REST client's `process` method for the model's streaming shape.
     *
     * @param binding the RestBinding containing the PipelineStepModel and service package information
     * @param ctx the GenerationContext providing generation-time metadata (for example, the DeploymentRole)
     * @param restClientInterfaceName simple name of the previously generated REST client interface to reference
     * @return a TypeSpec describing the generated REST client step class
     */
    private TypeSpec buildRestClientStepClass(RestBinding binding, GenerationContext ctx, String restClientInterfaceName) {
        PipelineStepModel model = binding.model();
        DeploymentRole role = ctx.role();
        String clientStepClassName = ResourceNameUtils.normalizeBaseName(model.generatedName())
            + PipelineStepProcessor.REST_CLIENT_STEP_SUFFIX;

        TypeName inputDto = model.inboundDomainType() != null
            ? convertDomainToDtoType(model.inboundDomainType())
            : ClassName.OBJECT;
        TypeName outputDto = model.outboundDomainType() != null
            ? convertDomainToDtoType(model.outboundDomainType())
            : ClassName.OBJECT;

        TypeSpec.Builder clientStepBuilder = TypeSpec.classBuilder(clientStepClassName)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.enterprise.context", "Dependent"))
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get(Unremovable.class))
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.pipelineframework.annotation", "ParallelismHint"))
                .addMember("ordering", "$T.$L",
                    ClassName.get(OrderingRequirement.class),
                    model.orderingRequirement().name())
                .addMember("threadSafety", "$T.$L",
                    ClassName.get(ThreadSafety.class),
                    model.threadSafety().name())
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.pipelineframework.annotation", "GeneratedRole"))
                .addMember("value", "$T.$L",
                    ClassName.get("org.pipelineframework.annotation", "GeneratedRole", "Role"),
                    role.name())
                .build());
        if (model.sideEffect()) {
            clientStepBuilder.addSuperinterface(ClassName.get("org.pipelineframework.cache", "CacheReadBypass"));
        }

        TypeName restClientInterface = ClassName.get(
            binding.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
            restClientInterfaceName);

        FieldSpec restClientField = FieldSpec.builder(restClientInterface, "restClient")
            .addAnnotation(AnnotationSpec.builder(
                    ClassName.get("jakarta.inject", "Inject"))
                .build())
            .addAnnotation(AnnotationSpec.builder(
                    ClassName.get("org.eclipse.microprofile.rest.client.inject", "RestClient"))
                .build())
            .build();

        clientStepBuilder.addField(restClientField);
        MethodSpec constructor = MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .build();
        clientStepBuilder.addMethod(constructor);

        ClassName configurableStep = ClassName.get("org.pipelineframework.step", "ConfigurableStep");
        clientStepBuilder.superclass(configurableStep);

        switch (model.streamingShape()) {
            case UNARY_UNARY -> {
                clientStepBuilder.addSuperinterface(ClassName.get("org.pipelineframework.cache", "CacheKeyTarget"));
                clientStepBuilder.addSuperinterface(ParameterizedTypeName.get(
                    ClassName.get(StepOneToOne.class), inputDto, outputDto));
                MethodSpec cacheKeyTargetMethod = MethodSpec.methodBuilder("cacheKeyTargetType")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(ParameterizedTypeName.get(ClassName.get(Class.class),
                        WildcardTypeName.subtypeOf(Object.class)))
                    .addStatement("return $T.class", outputDto)
                    .build();
                clientStepBuilder.addMethod(cacheKeyTargetMethod);
                MethodSpec.Builder applyOneToOneMethod = MethodSpec.methodBuilder("applyOneToOne")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(ParameterizedTypeName.get(ClassName.get(Uni.class), outputDto))
                    .addParameter(inputDto, "input")
                    .addStatement("$T context = $T.get()",
                        ClassName.get("org.pipelineframework.context", "PipelineContext"),
                        ClassName.get("org.pipelineframework.context", "PipelineContextHolder"))
                    .addStatement("String versionTag = context != null ? context.versionTag() : null")
                    .addStatement("String replayMode = context != null ? context.replayMode() : null")
                    .addStatement("String cachePolicy = context != null ? context.cachePolicy() : null");
                applyOneToOneMethod.addStatement(
                    "return $T.instrumentClient($S, $S, this.restClient.process(versionTag, replayMode, cachePolicy, input))",
                    ClassName.get("org.pipelineframework.telemetry", "HttpMetrics"),
                    model.serviceName(),
                    "process");
                clientStepBuilder.addMethod(applyOneToOneMethod.build());
            }
            case UNARY_STREAMING -> {
                ClassName stepInterface = ClassName.get("org.pipelineframework.step", "StepOneToMany");
                clientStepBuilder.addSuperinterface(ParameterizedTypeName.get(stepInterface, inputDto, outputDto));
                MethodSpec applyOneToManyMethod = MethodSpec.methodBuilder("applyOneToMany")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(ParameterizedTypeName.get(ClassName.get(Multi.class), outputDto))
                    .addParameter(inputDto, "input")
                    .addStatement("$T context = $T.get()",
                        ClassName.get("org.pipelineframework.context", "PipelineContext"),
                        ClassName.get("org.pipelineframework.context", "PipelineContextHolder"))
                    .addStatement("String versionTag = context != null ? context.versionTag() : null")
                    .addStatement("String replayMode = context != null ? context.replayMode() : null")
                    .addStatement("String cachePolicy = context != null ? context.cachePolicy() : null")
                    .addStatement(
                        "return $T.instrumentClient($S, $S, this.restClient.process(versionTag, replayMode, cachePolicy, input))",
                        ClassName.get("org.pipelineframework.telemetry", "HttpMetrics"),
                        model.serviceName(),
                        "process")
                    .build();
                clientStepBuilder.addMethod(applyOneToManyMethod);
            }
            case STREAMING_UNARY -> {
                clientStepBuilder.addSuperinterface(ParameterizedTypeName.get(
                    ClassName.get(StepManyToOne.class), inputDto, outputDto));
                MethodSpec applyBatchMultiMethod = MethodSpec.methodBuilder("applyBatchMulti")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(ParameterizedTypeName.get(ClassName.get(Uni.class), outputDto))
                    .addParameter(ParameterizedTypeName.get(ClassName.get(Multi.class), inputDto), "inputs")
                    .addStatement("$T context = $T.get()",
                        ClassName.get("org.pipelineframework.context", "PipelineContext"),
                        ClassName.get("org.pipelineframework.context", "PipelineContextHolder"))
                    .addStatement("String versionTag = context != null ? context.versionTag() : null")
                    .addStatement("String replayMode = context != null ? context.replayMode() : null")
                    .addStatement("String cachePolicy = context != null ? context.cachePolicy() : null")
                    .addStatement(
                        "return $T.instrumentClient($S, $S, this.restClient.process(versionTag, replayMode, cachePolicy, inputs))",
                        ClassName.get("org.pipelineframework.telemetry", "HttpMetrics"),
                        model.serviceName(),
                        "process")
                    .build();
                clientStepBuilder.addMethod(applyBatchMultiMethod);
            }
            case STREAMING_STREAMING -> {
                ClassName stepInterface = ClassName.get("org.pipelineframework.step", "StepManyToMany");
                clientStepBuilder.addSuperinterface(ParameterizedTypeName.get(stepInterface, inputDto, outputDto));
                MethodSpec applyTransformMethod = MethodSpec.methodBuilder("applyTransform")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(ParameterizedTypeName.get(ClassName.get(Multi.class), outputDto))
                    .addParameter(ParameterizedTypeName.get(ClassName.get(Multi.class), inputDto), "inputs")
                    .addStatement("$T context = $T.get()",
                        ClassName.get("org.pipelineframework.context", "PipelineContext"),
                        ClassName.get("org.pipelineframework.context", "PipelineContextHolder"))
                    .addStatement("String versionTag = context != null ? context.versionTag() : null")
                    .addStatement("String replayMode = context != null ? context.replayMode() : null")
                    .addStatement("String cachePolicy = context != null ? context.cachePolicy() : null")
                    .addStatement(
                        "return $T.instrumentClient($S, $S, this.restClient.process(versionTag, replayMode, cachePolicy, inputs))",
                        ClassName.get("org.pipelineframework.telemetry", "HttpMetrics"),
                        model.serviceName(),
                        "process")
                    .build();
                clientStepBuilder.addMethod(applyTransformMethod);
            }
        }

        return clientStepBuilder.build();
    }

    /**
     * Builds a MethodSpec for a REST client `process` method that accepts three pipeline header parameters
     * (`versionTag`, `replayMode`, `cachePolicy`) and an input DTO, and returns a `Uni` of the output DTO.
     *
     * @param inputDto  the DTO type used as the method's request entity
     * @param outputDto the DTO type used as the method's response entity
     * @return          a MethodSpec for the abstract `process` REST method (POST /process) returning `Uni<outputDto>`
     */
    private MethodSpec buildUnaryUnaryMethod(TypeName inputDto, TypeName outputDto) {
        MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("process")
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "POST")).build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Path"))
                .addMember("value", "$S", "/process")
                .build())
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(ParameterizedTypeName.get(ClassName.get(Uni.class), outputDto))
            .addParameter(headerParam("versionTag"))
            .addParameter(headerParam("replayMode"))
            .addParameter(headerParam("cachePolicy"))
            .addParameter(inputDto, "inputDto");
        return methodBuilder.build();
    }

    /**
     * Declares the REST endpoint for a unary-to-streaming operation: accepts a single input DTO and produces a stream of output DTOs.
     *
     * @param versionTag pipeline version tag header value
     * @param replayMode replay mode header value
     * @param cachePolicy cache policy header value
     * @param inputDto the input DTO to process
     * @return a Multi that emits output DTOs produced by processing the input DTO
     */
    private MethodSpec buildUnaryStreamingMethod(TypeName inputDto, TypeName outputDto) {
        MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("process")
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "POST")).build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Path"))
                .addMember("value", "$S", "/process")
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.jboss.resteasy.reactive", "RestStreamElementType"))
                .addMember("value", "$S", "application/json")
                .build())
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(ParameterizedTypeName.get(ClassName.get(Multi.class), outputDto))
            .addParameter(headerParam("versionTag"))
            .addParameter(headerParam("replayMode"))
            .addParameter(headerParam("cachePolicy"))
            .addParameter(inputDto, "inputDto");
        return methodBuilder.build();
    }

    /**
     * Declares the REST client's streaming-to-unary process operation.
     *
     * @param versionTag the pipeline version tag passed via header
     * @param replayMode the replay mode flag passed via header
     * @param cachePolicy the cache policy passed via header
     * @param inputDtos a stream of input DTOs to be processed
     * @return a Uni that emits the single output DTO produced from the streamed inputs
     */
    private MethodSpec buildStreamingUnaryMethod(TypeName inputDto, TypeName outputDto) {
        MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("process")
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "POST")).build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Path"))
                .addMember("value", "$S", "/process")
                .build())
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(ParameterizedTypeName.get(ClassName.get(Uni.class), outputDto))
            .addParameter(headerParam("versionTag"))
            .addParameter(headerParam("replayMode"))
            .addParameter(headerParam("cachePolicy"))
            .addParameter(ParameterSpec.builder(
                ParameterizedTypeName.get(ClassName.get(Multi.class), inputDto), "inputDtos").build());
        return methodBuilder.build();
    }

    /**
     * Builds a MethodSpec for an abstract REST client method representing a streaming-request, streaming-response
     * "process" endpoint annotated for NDJSON and JSON stream element type.
     *
     * @param inputDto  the DTO type used for each inbound stream element
     * @param outputDto the DTO type used for each outbound stream element
     * @return a MethodSpec for a public abstract `process` method that accepts `versionTag`, `replayMode`, `cachePolicy`
     *         header parameters and a `Multi<inputDto>` named `inputDtos`, returning `Multi<outputDto>`
     */
    private MethodSpec buildStreamingStreamingMethod(TypeName inputDto, TypeName outputDto) {
        MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("process")
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "POST")).build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Path"))
                .addMember("value", "$S", "/process")
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Consumes"))
                .addMember("value", "$S", "application/x-ndjson")
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Produces"))
                .addMember("value", "$S", "application/x-ndjson")
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.jboss.resteasy.reactive", "RestStreamElementType"))
                .addMember("value", "$S", "application/json")
                .build())
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(ParameterizedTypeName.get(ClassName.get(Multi.class), outputDto))
            .addParameter(headerParam("versionTag"))
            .addParameter(headerParam("replayMode"))
            .addParameter(headerParam("cachePolicy"))
            .addParameter(ParameterizedTypeName.get(ClassName.get(Multi.class), inputDto), "inputDtos");
        return methodBuilder.build();
    }

    /**
     * Builds a ParameterSpec for a `String` parameter annotated with `@HeaderParam` that references
     * the appropriate `PipelineContextHeaders` constant for the given logical header name.
     *
     * Recognized mappings:
     * - "versionTag" -> `PipelineContextHeaders.VERSION`
     * - "replayMode" -> `PipelineContextHeaders.REPLAY`
     * - "cachePolicy" -> `PipelineContextHeaders.CACHE_POLICY`
     * Unrecognized names default to `PipelineContextHeaders.VERSION`.
     *
     * @param name the logical header name to map (e.g., "versionTag", "replayMode", "cachePolicy")
     * @return a `ParameterSpec` for a `String` parameter named `name` annotated with `@HeaderParam` whose value is the mapped `PipelineContextHeaders` constant
     */
    private ParameterSpec headerParam(String name) {
        String headerConst = switch (name) {
            case "versionTag" -> "VERSION";
            case "replayMode" -> "REPLAY";
            case "cachePolicy" -> "CACHE_POLICY";
            default -> "VERSION";
        };
        return ParameterSpec.builder(String.class, name)
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "HeaderParam"))
                .addMember("value", "$T." + headerConst,
                    ClassName.get("org.pipelineframework.context", "PipelineContextHeaders"))
                .build())
            .build();
    }

    private String deriveResourcePath(String className) {
        className = ResourceNameUtils.normalizeBaseName(className);

        String pathPart = className.replaceAll("([a-z])([A-Z])", "$1-$2")
            .replaceAll("([A-Z]+)([A-Z][a-z])", "$1-$2")
            .toLowerCase();

        return "/api/v1/" + pathPart;
    }

    private TypeName convertDomainToDtoType(TypeName domainType) {
        return DtoTypeUtils.toDtoType(domainType);
    }

    private void validateRestMappings(PipelineStepModel model) {
        if (model == null) {
            throw new IllegalStateException("REST client generation requires a non-null PipelineStepModel");
        }
        if (model.inputMapping() == null || model.outputMapping() == null) {
            throw new IllegalStateException(String.format(
                "REST client generation for '%s' requires input/output mappings to be present",
                model.serviceName()));
        }
        if (!model.inputMapping().hasMapper() || model.inputMapping().domainType() == null) {
            throw new IllegalStateException(String.format(
                "REST client generation for '%s' requires a non-null input domain type and inbound mapper",
                model.serviceName()));
        }
        if (!model.outputMapping().hasMapper() || model.outputMapping().domainType() == null) {
            throw new IllegalStateException(String.format(
                "REST client generation for '%s' requires a non-null output domain type and outbound mapper",
                model.serviceName()));
        }
    }

    private static String toRestClientName(String serviceName) {
        if (serviceName == null || serviceName.isBlank()) {
            return "";
        }

        String baseName = serviceName.replaceFirst("Service$", "");
        String withBoundaryHyphens = baseName
            .replaceAll("([A-Z]+)([A-Z][a-z])", "$1-$2")
            .replaceAll("([a-z0-9])([A-Z])", "$1-$2");
        return withBoundaryHyphens.toLowerCase();
    }
}
