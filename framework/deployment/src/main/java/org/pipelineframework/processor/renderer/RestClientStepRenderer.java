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

    private TypeSpec buildRestClientInterface(RestBinding binding) {
        PipelineStepModel model = binding.model();
        validateRestMappings(model);

        String serviceClassName = model.generatedName();
        String baseName = serviceClassName.replace("Service", "");
        if (baseName.endsWith("Reactive")) {
            baseName = baseName.substring(0, baseName.length() - "Reactive".length());
        }
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

        TypeSpec.Builder interfaceBuilder = TypeSpec.interfaceBuilder(interfaceName)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(registerRestClient)
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

    private TypeSpec buildRestClientStepClass(RestBinding binding, GenerationContext ctx, String restClientInterfaceName) {
        PipelineStepModel model = binding.model();
        DeploymentRole role = ctx.role();
        String clientStepClassName = model.generatedName().replace("Service", "")
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
                    ClassName.get("org.pipelineframework.annotation.GeneratedRole", "Role"),
                    role.name())
                .build());

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
                clientStepBuilder.addSuperinterface(ParameterizedTypeName.get(
                    ClassName.get(StepOneToOne.class), inputDto, outputDto));
                MethodSpec.Builder applyOneToOneMethod = MethodSpec.methodBuilder("applyOneToOne")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(ParameterizedTypeName.get(ClassName.get(Uni.class), outputDto))
                    .addParameter(inputDto, "input");
                applyOneToOneMethod.addStatement(
                    "return $T.instrumentClient($S, $S, this.restClient.process(input))",
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
                    .addStatement(
                        "return $T.instrumentClient($S, $S, this.restClient.process(input))",
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
                    .addStatement(
                        "return $T.instrumentClient($S, $S, this.restClient.process(inputs))",
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
                    .addStatement(
                        "return $T.instrumentClient($S, $S, this.restClient.process(inputs))",
                        ClassName.get("org.pipelineframework.telemetry", "HttpMetrics"),
                        model.serviceName(),
                        "process")
                    .build();
                clientStepBuilder.addMethod(applyTransformMethod);
            }
        }

        return clientStepBuilder.build();
    }

    private MethodSpec buildUnaryUnaryMethod(TypeName inputDto, TypeName outputDto) {
        return MethodSpec.methodBuilder("process")
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "POST")).build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Path"))
                .addMember("value", "$S", "/process")
                .build())
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(ParameterizedTypeName.get(ClassName.get(Uni.class), outputDto))
            .addParameter(inputDto, "inputDto")
            .build();
    }

    private MethodSpec buildUnaryStreamingMethod(TypeName inputDto, TypeName outputDto) {
        return MethodSpec.methodBuilder("process")
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "POST")).build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Path"))
                .addMember("value", "$S", "/process")
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.jboss.resteasy.reactive", "RestStreamElementType"))
                .addMember("value", "$S", "application/json")
                .build())
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(ParameterizedTypeName.get(ClassName.get(Multi.class), outputDto))
            .addParameter(inputDto, "inputDto")
            .build();
    }

    private MethodSpec buildStreamingUnaryMethod(TypeName inputDto, TypeName outputDto) {
        return MethodSpec.methodBuilder("process")
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "POST")).build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Path"))
                .addMember("value", "$S", "/process")
                .build())
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(ParameterizedTypeName.get(ClassName.get(Uni.class), outputDto))
            .addParameter(ParameterSpec.builder(
                ParameterizedTypeName.get(ClassName.get(Multi.class), inputDto), "inputDtos").build())
            .build();
    }

    private MethodSpec buildStreamingStreamingMethod(TypeName inputDto, TypeName outputDto) {
        return MethodSpec.methodBuilder("process")
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
            .addParameter(ParameterizedTypeName.get(ClassName.get(Multi.class), inputDto), "inputDtos")
            .build();
    }

    private String deriveResourcePath(String className) {
        if (className.endsWith("Service")) {
            className = className.substring(0, className.length() - 7);
        }

        className = className.replace("Reactive", "");

        String pathPart = className.replaceAll("([a-z])([A-Z])", "$1-$2")
            .replaceAll("([A-Z]+)([A-Z][a-z])", "$1-$2")
            .toLowerCase();

        return "/api/v1/" + pathPart;
    }

    private TypeName convertDomainToDtoType(TypeName domainType) {
        if (domainType == null) {
            return ClassName.OBJECT;
        }

        String domainTypeStr = domainType.toString();
        String dtoTypeStr = domainTypeStr
            .replace(".domain.", ".dto.")
            .replace(".service.", ".dto.");

        if (!dtoTypeStr.equals(domainTypeStr)) {
            int lastDot = dtoTypeStr.lastIndexOf('.');
            String packageName = lastDot > 0 ? dtoTypeStr.substring(0, lastDot) : "";
            String simpleName = lastDot > 0 ? dtoTypeStr.substring(lastDot + 1) : dtoTypeStr;
            String dtoSimpleName = simpleName + "Dto";
            return ClassName.get(packageName, dtoSimpleName);
        }

        int lastDot = domainTypeStr.lastIndexOf('.');
        String packageName = lastDot > 0 ? domainTypeStr.substring(0, lastDot) : "";
        String simpleName = lastDot > 0 ? domainTypeStr.substring(lastDot + 1) : domainTypeStr;
        String dtoSimpleName = simpleName + "Dto";
        return ClassName.get(packageName, dtoSimpleName);
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
