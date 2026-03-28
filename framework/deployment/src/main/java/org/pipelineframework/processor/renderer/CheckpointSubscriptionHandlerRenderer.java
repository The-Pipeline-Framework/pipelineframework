package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import io.smallrye.mutiny.Uni;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import org.pipelineframework.config.boundary.PipelineSubscriptionConfig;
import org.pipelineframework.processor.ir.OrchestratorBinding;

/**
 * Generates a subscriber-side admission handler for one logical checkpoint publication.
 */
public class CheckpointSubscriptionHandlerRenderer {

    private static final String MAPPER_INTERFACE = "org.pipelineframework.mapper.Mapper";

    public ClassName render(OrchestratorBinding binding, PipelineSubscriptionConfig subscription, GenerationContext ctx)
        throws IOException {
        ClassName generatedType = ClassName.get(binding.basePackage() + ".orchestrator.service",
            "PipelineCheckpointSubscriptionHandler");
        ClassName handlerType = ClassName.get("org.pipelineframework.checkpoint", "CheckpointSubscriptionHandler");
        ClassName pipelineExecutionService = ClassName.get("org.pipelineframework", "PipelineExecutionService");
        ClassName jsonNode = ClassName.get("com.fasterxml.jackson.databind", "JsonNode");
        ClassName objectMapper = ClassName.get("com.fasterxml.jackson.databind", "ObjectMapper");
        ClassName pipelineJson = ClassName.get("org.pipelineframework.config.pipeline", "PipelineJson");
        ClassName runAsyncAcceptedDto = ClassName.get("org.pipelineframework.orchestrator.dto", "RunAsyncAcceptedDto");
        ClassName uni = ClassName.get(Uni.class);
        TypeName directInputType = resolveDirectInputType(binding, ctx);
        ClassName mapperInterface = ClassName.get("org.pipelineframework.mapper", "Mapper");

        TypeSpec.Builder type = TypeSpec.classBuilder(generatedType)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(ClassName.get("jakarta.enterprise.context", "Dependent"))
            .addAnnotation(ClassName.get("io.quarkus.arc", "Unremovable"))
            .addSuperinterface(handlerType)
            .addField(FieldSpec.builder(objectMapper, "JSON", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                .initializer("$T.mapper()", pipelineJson)
                .build())
            .addField(FieldSpec.builder(pipelineExecutionService, "pipelineExecutionService", Modifier.PRIVATE)
                .addAnnotation(ClassName.get("jakarta.inject", "Inject"))
                .build());

        boolean hasMapper = subscription.mapper() != null && !subscription.mapper().isBlank();
        ClassName mapperType = null;
        TypeName mapperDomainType = null;
        TypeName mapperExternalType = directInputType;
        if (hasMapper) {
            mapperType = ClassName.bestGuess(subscription.mapper());
            MapperTypes mapperTypes = resolveMapperTypes(subscription.mapper(), ctx);
            mapperDomainType = mapperTypes.domainType();
            mapperExternalType = mapperTypes.externalType();
            type.addField(FieldSpec.builder(mapperType, "mapper", Modifier.PRIVATE)
                .addAnnotation(ClassName.get("jakarta.inject", "Inject"))
                .build());
        }

        type.addMethod(MethodSpec.methodBuilder("publication")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(String.class)
            .addStatement("return $S", subscription.publication())
            .build());

        MethodSpec.Builder admit = MethodSpec.methodBuilder("admit")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(uni, runAsyncAcceptedDto))
            .addParameter(jsonNode, "payload")
            .addParameter(String.class, "tenantId")
            .addParameter(String.class, "idempotencyKey")
            .beginControlFlow("try");
        if (hasMapper) {
            admit.addStatement("$T domain = JSON.treeToValue(payload, $T.class)", mapperDomainType, mapperDomainType)
                .addStatement("Object mapped = mapper.toExternal(domain)")
                .addStatement("return pipelineExecutionService.executePipelineAsync(mapped, tenantId, idempotencyKey)");
        } else {
            admit.addStatement("$T mapped = JSON.treeToValue(payload, $T.class)", directInputType, directInputType)
                .addStatement("return pipelineExecutionService.executePipelineAsync(mapped, tenantId, idempotencyKey)");
        }
        admit.nextControlFlow("catch ($T e)", Exception.class)
            .addStatement("return $T.createFrom().failure(e)", uni)
            .endControlFlow();
        type.addMethod(admit.build());

        JavaFile.builder(generatedType.packageName(), type.build())
            .build()
            .writeTo(ctx.processingEnv().getFiler());
        return generatedType;
    }

    private String simpleTypeName(String typeName) {
        if (typeName == null || typeName.isBlank()) {
            throw new IllegalStateException("Checkpoint subscription input type name must not be blank");
        }
        int lastDot = typeName.lastIndexOf('.');
        return lastDot >= 0 ? typeName.substring(lastDot + 1) : typeName;
    }

    private TypeName resolveDirectInputType(OrchestratorBinding binding, GenerationContext ctx) {
        if (binding.model() != null
            && binding.model().inputMapping() != null
            && binding.model().inputMapping().domainType() != null) {
            return binding.model().inputMapping().domainType();
        }
        TypeElement serviceElement = resolveFirstStepServiceElement(binding, ctx);
        if (serviceElement != null) {
            TypeName annotatedInputType = resolveAnnotatedInputType(serviceElement);
            if (annotatedInputType != null) {
                return annotatedInputType;
            }
        }
        return ClassName.get(
            binding.basePackage() + ".common.dto",
            simpleTypeName(binding.inputTypeName()) + "Dto");
    }

    private TypeElement resolveFirstStepServiceElement(OrchestratorBinding binding, GenerationContext ctx) {
        if (ctx.processingEnv() == null) {
            return null;
        }
        if (binding.model() != null && binding.model().serviceClassName() != null) {
            TypeElement direct = ctx.processingEnv()
                .getElementUtils()
                .getTypeElement(binding.model().serviceClassName().canonicalName());
            if (direct != null) {
                return direct;
            }
        }
        String serviceName = binding.firstStepServiceName();
        if (serviceName == null || serviceName.isBlank()) {
            return null;
        }
        String inferredServicePackage = binding.basePackage() + "." + toPackageSegment(serviceName) + ".service";
        return ctx.processingEnv()
            .getElementUtils()
            .getTypeElement(inferredServicePackage + "." + serviceName);
    }

    private TypeName resolveAnnotatedInputType(TypeElement serviceElement) {
        for (var annotation : serviceElement.getAnnotationMirrors()) {
            if (!"org.pipelineframework.annotation.PipelineStep".equals(annotation.getAnnotationType().toString())) {
                continue;
            }
            for (var entry : annotation.getElementValues().entrySet()) {
                if (!"inputType".equals(entry.getKey().getSimpleName().toString())) {
                    continue;
                }
                Object value = entry.getValue().getValue();
                if (value instanceof TypeMirror typeMirror) {
                    return TypeName.get(typeMirror);
                }
            }
        }
        return null;
    }

    private String toPackageSegment(String serviceName) {
        String normalized = serviceName;
        if (normalized.startsWith("Process") && normalized.length() > "Process".length()) {
            normalized = normalized.substring("Process".length());
        }
        if (normalized.endsWith("Service") && normalized.length() > "Service".length()) {
            normalized = normalized.substring(0, normalized.length() - "Service".length());
        }
        if (normalized.isBlank()) {
            return "service";
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < normalized.length(); i++) {
            char current = normalized.charAt(i);
            if (Character.isUpperCase(current) && i > 0) {
                builder.append('_');
            }
            builder.append(Character.toLowerCase(current));
        }
        String sanitized = builder.toString().replaceAll("[^a-z0-9_]+", "_");
        if (!sanitized.isBlank() && Character.isDigit(sanitized.charAt(0))) {
            sanitized = "step_" + sanitized;
        }
        return sanitized.isBlank() ? "service" : sanitized.toLowerCase(Locale.ROOT);
    }

    private MapperTypes resolveMapperTypes(String mapperClassName, GenerationContext ctx) {
        TypeMirror mapperInterface = Objects.requireNonNull(
            ctx.processingEnv().getElementUtils().getTypeElement(MAPPER_INTERFACE),
            () -> "Mapper interface not found: " + MAPPER_INTERFACE).asType();
        javax.lang.model.element.TypeElement mapperElement = ctx.processingEnv().getElementUtils().getTypeElement(mapperClassName);
        if (mapperElement == null) {
            throw new IllegalStateException("Checkpoint subscription mapper type not found: " + mapperClassName);
        }
        for (TypeMirror implemented : mapperElement.getInterfaces()) {
            if (!(implemented instanceof DeclaredType declared)
                || !ctx.processingEnv().getTypeUtils().isSameType(
                    ctx.processingEnv().getTypeUtils().erasure(declared),
                    ctx.processingEnv().getTypeUtils().erasure(mapperInterface))) {
                continue;
            }
            if (declared.getTypeArguments().size() == 2) {
                return new MapperTypes(
                    TypeName.get(declared.getTypeArguments().get(0)),
                    TypeName.get(declared.getTypeArguments().get(1)));
            }
        }
        throw new IllegalStateException(
            "Checkpoint subscription mapper '" + mapperClassName + "' must declare Mapper<Domain, External>");
    }

    private record MapperTypes(TypeName domainType, TypeName externalType) {
    }
}
