package org.pipelineframework.processor.renderer;

import java.io.IOException;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.RestBinding;
import org.pipelineframework.processor.ir.StreamingShape;

/**
 * Generates native AWS Lambda RequestHandler wrappers for unary REST resources.
 */
public class RestFunctionHandlerRenderer implements PipelineRenderer<RestBinding> {

    /**
     * Creates a new RestFunctionHandlerRenderer.
     */
    public RestFunctionHandlerRenderer() {
    }

    @Override
    public GenerationTarget target() {
        return GenerationTarget.REST_RESOURCE;
    }

    @Override
    public void render(RestBinding binding, GenerationContext ctx) throws IOException {
        PipelineStepModel model = binding.model();
        if (model.streamingShape() != StreamingShape.UNARY_UNARY) {
            throw new IllegalStateException(
                "Function step handler currently supports only UNARY_UNARY shape.");
        }

        ClassName applicationScoped = ClassName.get("jakarta.enterprise.context", "ApplicationScoped");
        ClassName inject = ClassName.get("jakarta.inject", "Inject");
        ClassName named = ClassName.get("jakarta.inject", "Named");
        ClassName lambdaContext = ClassName.get("com.amazonaws.services.lambda.runtime", "Context");
        ClassName requestHandler = ClassName.get("com.amazonaws.services.lambda.runtime", "RequestHandler");
        ClassName generatedRole = ClassName.get("org.pipelineframework.annotation", "GeneratedRole");
        ClassName roleEnum = ClassName.get("org.pipelineframework.annotation.GeneratedRole", "Role");

        String serviceClassName = model.generatedName();
        String baseName = removeSuffix(removeSuffix(serviceClassName, "Service"), "Reactive");
        String resourceClassName = baseName + PipelineStepProcessor.REST_RESOURCE_SUFFIX;
        String handlerClassName = baseName + "FunctionHandler";

        TypeName inputDto = model.inboundDomainType() != null
            ? convertDomainToDtoType(model.inboundDomainType())
            : ClassName.OBJECT;
        TypeName outputDto = model.outboundDomainType() != null
            ? convertDomainToDtoType(model.outboundDomainType())
            : ClassName.OBJECT;
        ClassName resourceType = ClassName.get(
            binding.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
            resourceClassName);

        TypeSpec.Builder handler = TypeSpec.classBuilder(handlerClassName)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(applicationScoped)
            .addAnnotation(AnnotationSpec.builder(named)
                .addMember("value", "$S", handlerClassName)
                .build())
            .addAnnotation(AnnotationSpec.builder(generatedRole)
                .addMember("value", "$T.$L", roleEnum, "REST_SERVER")
                .build())
            .addSuperinterface(ParameterizedTypeName.get(requestHandler, inputDto, outputDto))
            .addField(com.squareup.javapoet.FieldSpec.builder(resourceType, "resource", Modifier.PRIVATE)
                .addAnnotation(inject)
                .build());

        com.squareup.javapoet.MethodSpec handleRequest = com.squareup.javapoet.MethodSpec.methodBuilder("handleRequest")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(outputDto)
            .addParameter(inputDto, "input")
            .addParameter(lambdaContext, "context")
            .addStatement("return resource.process(input).await().indefinitely()")
            .build();
        handler.addMethod(handleRequest);

        JavaFile.builder(binding.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX, handler.build())
            .build()
            .writeTo(ctx.outputDir());
    }

    private TypeName convertDomainToDtoType(TypeName domainType) {
        if (domainType == null) {
            return ClassName.OBJECT;
        }
        if (domainType instanceof ClassName className) {
            String dtoSimpleName = className.simpleName().endsWith("Dto")
                ? className.simpleName()
                : className.simpleName() + "Dto";
            return ClassName.get(rewritePackageToDto(className.packageName()), dtoSimpleName);
        }

        String domainTypeStr = domainType.toString();
        int lastDot = domainTypeStr.lastIndexOf('.');
        String packageName = lastDot > 0 ? domainTypeStr.substring(0, lastDot) : "";
        String simpleName = lastDot > 0 ? domainTypeStr.substring(lastDot + 1) : domainTypeStr;
        String dtoSimpleName = simpleName.endsWith("Dto") ? simpleName : simpleName + "Dto";
        return ClassName.get(rewritePackageToDto(packageName), dtoSimpleName);
    }

    private String rewritePackageToDto(String packageName) {
        if (packageName == null || packageName.isBlank()) {
            return "";
        }
        String[] segments = packageName.split("\\.");
        for (int i = 0; i < segments.length; i++) {
            if ("domain".equals(segments[i]) || "service".equals(segments[i])) {
                segments[i] = "dto";
                break;
            }
        }
        return String.join(".", segments);
    }

    private String removeSuffix(String value, String suffix) {
        if (value == null || suffix == null || suffix.isBlank()) {
            return value;
        }
        return value.endsWith(suffix) ? value.substring(0, value.length() - suffix.length()) : value;
    }
}
