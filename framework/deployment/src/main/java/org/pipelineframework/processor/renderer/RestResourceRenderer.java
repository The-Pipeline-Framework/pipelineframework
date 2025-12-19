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
import org.pipelineframework.processor.ir.PipelineStepIR;

/**
 * Renderer for REST resource implementations based on PipelineStepIR
 */
public class RestResourceRenderer implements PipelineRenderer {

    /**
     * Creates a new RestResourceRenderer.
     */
    public RestResourceRenderer() {
    }
    
    @Override
    public GenerationTarget target() {
        return GenerationTarget.REST_RESOURCE;
    }
    
    @Override
    public void render(PipelineStepIR ir, GenerationContext ctx) throws IOException {
        TypeSpec restResourceClass = buildRestResourceClass(ir, ctx.getProcessingEnv());
        
        // Write the generated class
        JavaFile javaFile = JavaFile.builder(
            ir.getServicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX, 
            restResourceClass)
            .build();

        try (var writer = ctx.getBuilderFile().openWriter()) {
            javaFile.writeTo(writer);
        }
    }
    
    private TypeSpec buildRestResourceClass(PipelineStepIR ir, javax.annotation.processing.ProcessingEnvironment processingEnv) {
        String serviceClassName = ir.getServiceName();
        
        // Determine the resource class name - remove "Service" and optionally "Reactive" for cleaner naming
        String baseName = serviceClassName.replace("Service", "");
        if (baseName.endsWith("Reactive")) {
            baseName = baseName.substring(0, baseName.length() - "Reactive".length());
        }
        String resourceClassName = baseName + PipelineStepProcessor.REST_RESOURCE_SUFFIX;

        // Create the REST resource class
        TypeSpec.Builder resourceBuilder = TypeSpec.classBuilder(resourceClassName)
            .addModifiers(Modifier.PUBLIC);

        // Add @Path annotation - derive path from service class name or use provided path
        String servicePath = ir.getRestPath() != null ? ir.getRestPath() : deriveResourcePath(serviceClassName);
        resourceBuilder.addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.ws.rs", "Path"))
            .addMember("value", "$S", servicePath)
            .build());

        // Add service field with @Inject
        FieldSpec serviceField = FieldSpec.builder(
            ir.getServiceClassName(),
            "domainService")
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.inject", "Inject")).build())
            .build();
        resourceBuilder.addField(serviceField);

        // Add mapper fields with @Inject if they exist
        String inboundMapperFieldName = "inboundMapper";
        String outboundMapperFieldName = "outboundMapper";

        if (ir.getInputMapping().hasMapper()) {
            String inboundMapperSimpleName = ir.getInputMapping().getMapperType().toString().substring(
                ir.getInputMapping().getMapperType().toString().lastIndexOf('.') + 1);
            inboundMapperFieldName = inboundMapperSimpleName.substring(0, 1).toLowerCase() + 
                inboundMapperSimpleName.substring(1);

            FieldSpec inboundMapperField = FieldSpec.builder(
                ir.getInputMapping().getMapperType(),
                inboundMapperFieldName)
                .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.inject", "Inject")).build())
                .build();
            resourceBuilder.addField(inboundMapperField);
        }

        if (ir.getOutputMapping().hasMapper()) {
            String outboundMapperSimpleName = ir.getOutputMapping().getMapperType().toString().substring(
                ir.getOutputMapping().getMapperType().toString().lastIndexOf('.') + 1);
            outboundMapperFieldName = outboundMapperSimpleName.substring(0, 1).toLowerCase() + 
                outboundMapperSimpleName.substring(1);

            FieldSpec outboundMapperField = FieldSpec.builder(
                ir.getOutputMapping().getMapperType(),
                outboundMapperFieldName)
                .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.inject", "Inject")).build())
                .build();
            resourceBuilder.addField(outboundMapperField);
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

        // For REST resources, we should use appropriate DTO types, not gRPC types
        // The DTO types should be derived from domain types (following the original approach)
        // using the same transformation logic as the original getDtoType method would have used
        TypeName inputDtoClassName = ir.getInputMapping().getDomainType() != null ?
            convertDomainToDtoType(ir.getInputMapping().getDomainType()) : ClassName.OBJECT;
        TypeName outputDtoClassName = ir.getOutputMapping().getDomainType() != null ?
            convertDomainToDtoType(ir.getOutputMapping().getDomainType()) : ClassName.OBJECT;

        // Create the process method based on service type (determined from streaming shape)
        MethodSpec processMethod;
        switch (ir.getStreamingShape()) {
            case UNARY_STREAMING:
                processMethod = createReactiveStreamingServiceProcessMethod(
                    inputDtoClassName, outputDtoClassName,
                    inboundMapperFieldName, outboundMapperFieldName, ir);
                break;
            case STREAMING_UNARY:
                processMethod = createReactiveStreamingClientServiceProcessMethod(
                    inputDtoClassName, outputDtoClassName,
                    inboundMapperFieldName, outboundMapperFieldName, ir);
                break;
            case STREAMING_STREAMING:
                processMethod = createReactiveBidirectionalStreamingServiceProcessMethod(
                    inputDtoClassName, outputDtoClassName,
                    inboundMapperFieldName, outboundMapperFieldName, ir);
                break;
            case UNARY_UNARY:
            default:
                processMethod = createReactiveServiceProcessMethod(
                    inputDtoClassName, outputDtoClassName,
                    inboundMapperFieldName, outboundMapperFieldName, ir);
                break;
        }

        resourceBuilder.addMethod(processMethod);

        // Add exception mapper method to handle different types of exceptions
        MethodSpec exceptionMapperMethod = createExceptionMapperMethod();
        resourceBuilder.addMethod(exceptionMapperMethod);

        return resourceBuilder.build();
    }
    
    private MethodSpec createReactiveServiceProcessMethod(
            TypeName inputDtoClassName, TypeName outputDtoClassName,
            String inboundMapperFieldName, String outboundMapperFieldName,
            PipelineStepIR ir) {

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
                ir.getInputMapping().getDomainType(),
                inboundMapperFieldName);

        methodBuilder.addStatement("return domainService.process(inputDomain).map(output -> $L.toDto(output))",
                outboundMapperFieldName);

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (ir.getExecutionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            methodBuilder.addAnnotation(ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        return methodBuilder.build();
    }

    private MethodSpec createReactiveStreamingServiceProcessMethod(
            TypeName inputDtoClassName, TypeName outputDtoClassName,
            String inboundMapperFieldName, String outboundMapperFieldName,
            PipelineStepIR ir) {

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
                ir.getInputMapping().getDomainType(),
                inboundMapperFieldName);

        // Return the stream, allowing errors to propagate to the exception mapper
        methodBuilder.addStatement("return domainService.process(inputDomain).map(output -> $L.toDto(output))",
                outboundMapperFieldName);

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (ir.getExecutionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            methodBuilder.addAnnotation(ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        return methodBuilder.build();
    }

    private MethodSpec createReactiveStreamingClientServiceProcessMethod(
            TypeName inputDtoClassName, TypeName outputDtoClassName,
            String inboundMapperFieldName, String outboundMapperFieldName,
            PipelineStepIR ir) {

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
                ir.getInputMapping().getDomainType(),
                inboundMapperFieldName);

        methodBuilder.addStatement("return domainService.process(domainInputs).map(output -> $L.toDto(output))",
                outboundMapperFieldName);

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (ir.getExecutionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            methodBuilder.addAnnotation(ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        return methodBuilder.build();
    }

    private MethodSpec createReactiveBidirectionalStreamingServiceProcessMethod(
            TypeName inputDtoClassName, TypeName outputDtoClassName,
            String inboundMapperFieldName, String outboundMapperFieldName,
            PipelineStepIR ir) {

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
                ir.getInputMapping().getDomainType(),
                inboundMapperFieldName);

        methodBuilder.addStatement("return domainService.process(domainInputs).map(output -> $L.toDto(output))",
                outboundMapperFieldName);

        // Add @RunOnVirtualThread annotation if the property is enabled
        if (ir.getExecutionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            methodBuilder.addAnnotation(ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        return methodBuilder.build();
    }
    
    private MethodSpec createExceptionMapperMethod() {
        return MethodSpec.methodBuilder("handleException")
            .addAnnotation(AnnotationSpec.builder(ClassName.get(ServerExceptionMapper.class))
                .build())
            .addModifiers(Modifier.PUBLIC)
            .returns(ClassName.get(RestResponse.class))
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
     * Converts a domain type name to its corresponding DTO type name.
     * This follows the common pattern of changing .domain. to .dto. in the package name
     * and adding "Dto" suffix to the class name (like the original getDtoType method would have done).
     *
     * @param domainType the domain type to convert
     * @return the corresponding DTO type name
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

            String dtoSimpleName = simpleName + "Dto";
            return ClassName.get(packageName, dtoSimpleName);
        }
    }
}