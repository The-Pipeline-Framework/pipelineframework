package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.WildcardTypeName;
import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.ir.DeferredCompletionSelection;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.GrpcBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.PipelineTransport;
import org.pipelineframework.processor.ir.TypeMapping;
import org.pipelineframework.processor.util.DtoTypeUtils;
import org.pipelineframework.processor.util.GrpcJavaTypeResolver;

/** Renders the durable completion modifier that follows an ordinary operation adapter. */
public class DeferredCompletionStepRenderer {

    public GenerationTarget target() {
        return GenerationTarget.DEFERRED_COMPLETION_STEP;
    }

    public void render(PipelineStepModel model, GenerationContext ctx) throws IOException {
        render(model, ctx, null);
    }

    public void render(PipelineStepModel model, GenerationContext ctx, GrpcBinding grpcBinding) throws IOException {
        DeferredCompletionSelection completion = model.deferredCompletionSelection().orElseThrow(() ->
            new IllegalArgumentException("Deferred completion target requires a resolved completion selection"));
        String baseName = stripService(model.generatedName());
        String className = baseName + "DeferredCompletionStep";
        String pipelinePackage = model.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX;
        TransportBoundaryResolver.RepresentationBoundary operationBoundary = operationBoundary(model, ctx, grpcBinding);
        TypeName operationOutputType = operationBoundary.stepOutputType();
        TypeName finalTransportType = clientStepType(
            completion.finalOutputType(), ctx.transportMode(), ctx.pipelineBasePackage());
        TypeName finalOutputType = operationBoundary.outputConvertsAtBoundary()
            ? completion.finalOutputType()
            : finalTransportType;
        CompletionBoundary completionBoundary = completionBoundary(completion, ctx, finalOutputType);

        FieldSpec support = FieldSpec.builder(
                ClassName.get("org.pipelineframework.awaitable", "AwaitCompletionSupport"), "completionSupport")
            .addAnnotation(ClassName.get("jakarta.inject", "Inject"))
            .build();
        FieldSpec descriptorFactory = FieldSpec.builder(
                ClassName.get("org.pipelineframework.awaitable", "AwaitCompletionDescriptorRegistry"), "descriptorRegistry")
            .addAnnotation(ClassName.get("jakarta.inject", "Inject"))
            .build();
        FieldSpec descriptor = FieldSpec.builder(
                ClassName.get("org.pipelineframework.awaitable", "AwaitCompletionDescriptor"), "completion")
            .addModifiers(Modifier.PRIVATE)
            .build();

        TypeSpec.Builder type = TypeSpec.classBuilder(className)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.enterprise.context", "Dependent")).build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get(Unremovable.class)).build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("io.quarkus.runtime", "Startup")).build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.pipelineframework.annotation", "GeneratedRole"))
                .addMember("value", "$T.$L",
                    ClassName.get("org.pipelineframework.annotation", "GeneratedRole", "Role"),
                    ctx.role().name())
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.pipelineframework.annotation", "ParallelismHint"))
                .addMember("ordering", "$T.$L", ClassName.get(OrderingRequirement.class), OrderingRequirement.RELAXED.name())
                .addMember("threadSafety", "$T.$L", ClassName.get(ThreadSafety.class), ThreadSafety.SAFE.name())
                .build())
            .superclass(ClassName.get("org.pipelineframework.step", "ConfigurableStep"))
            .addSuperinterface(ParameterizedTypeName.get(
                RuntimeSymbols.STEP_ONE_TO_ONE, operationOutputType, finalOutputType))
            .addSuperinterface(ParameterizedTypeName.get(
                ClassName.get("org.pipelineframework.awaitable", "AwaitStreamOneToOneStep"),
                operationOutputType,
                finalOutputType))
            .addSuperinterface(ClassName.get("org.pipelineframework.cache", "CacheReadBypass"))
            .addSuperinterface(ClassName.get("org.pipelineframework.cache", "CacheKeyTarget"))
            .addField(support)
            .addField(descriptorFactory)
            .addField(descriptor)
            .addMethod(MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC).build())
            .addMethod(registerDescriptor(
                model, completion, operationBoundary, completionBoundary, operationOutputType, finalOutputType))
            .addMethod(MethodSpec.methodBuilder("cacheKeyTargetType")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get(Class.class), WildcardTypeName.subtypeOf(Object.class)))
                .addStatement("return $T.class", finalOutputType)
                .build())
            .addMethod(MethodSpec.methodBuilder("applyOneToOne")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get(Uni.class), finalOutputType))
                .addParameter(operationOutputType, "operationOutput")
                .addStatement("return completionSupport.<$T, $T>awaitOneToOne(completion, operationOutput)",
                    operationOutputType, finalOutputType)
                .build())
            .addMethod(MethodSpec.methodBuilder("applyAwaitPerItem")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get(Multi.class), finalOutputType))
                .addParameter(ParameterizedTypeName.get(ClassName.get(Multi.class), operationOutputType), "input")
                .addStatement("return completionSupport.<$T, $T>awaitOneToOneStream(completion, input)",
                    operationOutputType, finalOutputType)
                .build());

        JavaFile.builder(pipelinePackage, type.build()).build().writeTo(ctx.outputDir());
    }

    private MethodSpec registerDescriptor(
        PipelineStepModel model,
        DeferredCompletionSelection completion,
        TransportBoundaryResolver.RepresentationBoundary operationBoundary,
        CompletionBoundary completionBoundary,
        TypeName operationOutputType,
        TypeName finalOutputType
    ) {
        CodeBlock inputToTransport = operationBoundary.outputConvertsAtBoundary()
            ? CodeBlock.of("value -> $T.toProto(($T) value)",
                operationBoundary.outputAdapterOrThrow(), operationOutputType)
            : CodeBlock.of("$T.identity()", Function.class);
        CodeBlock outputFromTransport = completionBoundary.adapter().isPresent()
            ? CodeBlock.of("value -> $T.fromProto(($T) value)",
                completionBoundary.adapter().orElseThrow(), completionBoundary.transportType())
            : CodeBlock.of("$T.identity()", Function.class);
        CodeBlock descriptor = completion.completionProjector().isPresent()
            ? CodeBlock.of(
                "new $T($S, $S, $S, $S, $T.parse($S), $S, $S, $L, $L, $S, $S, $L, $L, $S, "
                    + "($T<Object, Object, Object>) ($T<?, ?, ?>) new $T(), true)",
                ClassName.get("org.pipelineframework.awaitable", "AwaitCompletionDescriptor"),
                model.serviceName(), operationOutputType.toString(), finalOutputType.toString(), "ONE_TO_ONE",
                Duration.class, completion.timeout().toString(), completion.correlationStrategy(),
                completion.transportType(), JavaPoetLiteral.value(completion.transportConfig()),
                JavaPoetLiteral.value(completion.idempotencyKeyFields()),
                operationBoundary.transportOutputType().toString(),
                completionBoundary.transportType().toString(), inputToTransport, outputFromTransport,
                completion.completionProjector().orElseThrow().canonicalName(),
                ClassName.get("org.pipelineframework.awaitable", "AwaitCompletionProjector"),
                ClassName.get("org.pipelineframework.awaitable", "AwaitCompletionProjector"),
                completion.completionProjector().orElseThrow())
            : CodeBlock.of(
                "new $T($S, $S, $S, $S, $T.parse($S), $S, $S, $L, $L, $S, $S, $L, $L)",
                ClassName.get("org.pipelineframework.awaitable", "AwaitCompletionDescriptor"),
                model.serviceName(), operationOutputType.toString(), finalOutputType.toString(), "ONE_TO_ONE",
                Duration.class, completion.timeout().toString(), completion.correlationStrategy(),
                completion.transportType(), JavaPoetLiteral.value(completion.transportConfig()),
                JavaPoetLiteral.value(completion.idempotencyKeyFields()),
                operationBoundary.transportOutputType().toString(),
                completionBoundary.transportType().toString(), inputToTransport, outputFromTransport);
        return MethodSpec.methodBuilder("registerCompletion")
            .addAnnotation(ClassName.get("jakarta.annotation", "PostConstruct"))
            .addModifiers(Modifier.PUBLIC)
            .addStatement("completion = descriptorRegistry.register($L)", descriptor)
            .build();
    }

    private TransportBoundaryResolver.RepresentationBoundary operationBoundary(
        PipelineStepModel model,
        GenerationContext ctx,
        GrpcBinding grpcBinding
    ) throws IOException {
        PipelineTransport transport = ctx.transportMode() == null ? PipelineTransport.GRPC : ctx.transportMode();
        return switch (transport) {
            case LOCAL -> localOperationBoundary(model, ctx);
            case REST -> {
                CanonicalTransportBindingPair mapping = CanonicalTransportBindingResolver.resolveAndEnsure(
                    ctx, model, PipelineTransport.REST);
                yield TransportBoundaryResolver.RepresentationBoundary.transportOnly(
                    mapping.restInputOr(() -> DtoTypeUtils.toDtoType(model.inboundDomainType())),
                    mapping.restOutputOr(() -> DtoTypeUtils.toDtoType(model.outboundDomainType())));
            }
            case GRPC -> {
                if (grpcBinding == null) {
                    TypeName transportInput = clientStepType(
                        model.inboundDomainType(), PipelineTransport.GRPC, ctx.pipelineBasePackage());
                    TypeName transportOutput = clientStepType(
                        model.outboundDomainType(), PipelineTransport.GRPC, ctx.pipelineBasePackage());
                    yield TransportBoundaryResolver.resolveAwait(
                        model, transportInput, transportOutput, ctx.pipelineBasePackage(), ctx.v3GeneratedDomainTypes());
                }
                GrpcJavaTypeResolver.GrpcJavaTypes grpcTypes = new GrpcJavaTypeResolver().resolve(
                    grpcBinding, ctx.compilerDiagnostics());
                yield TransportBoundaryResolver.resolve(model, grpcTypes, ctx);
            }
        };
    }

    private TransportBoundaryResolver.RepresentationBoundary localOperationBoundary(
        PipelineStepModel model,
        GenerationContext context
    ) {
        if (!generatedV3DomainType(model.outboundDomainType(), context)) {
            return TransportBoundaryResolver.RepresentationBoundary.transportOnly(
                model.inboundDomainType(), model.outboundDomainType());
        }
        TypeName transportOutput = clientStepType(
            model.outboundDomainType(), PipelineTransport.GRPC, context.pipelineBasePackage());
        return new TransportBoundaryResolver.RepresentationBoundary(
            model.inboundDomainType(), model.outboundDomainType(), model.inboundDomainType(), transportOutput,
            Optional.empty(), Optional.of(generatedV3Adapter(context)));
    }

    private CompletionBoundary completionBoundary(
        DeferredCompletionSelection completion,
        GenerationContext context,
        TypeName finalOutputType
    ) throws IOException {
        TypeName domainType = completion.completionPayloadType().orElse(finalOutputType);
        String canonicalType = completion.completionPayloadCanonicalType()
            .orElse(completion.finalOutputCanonicalType());
        PipelineTransport transport = context.transportMode() == null
            ? PipelineTransport.GRPC : context.transportMode();
        if ((transport == PipelineTransport.LOCAL || transport == PipelineTransport.GRPC)
            && generatedV3DomainType(domainType, context)
            && domainType instanceof ClassName domain
            && domain.simpleName().equals(canonicalSimpleName(canonicalType))) {
            return new CompletionBoundary(
                ClassName.get(
                    context.pipelineBasePackage() + ".grpc", "PipelineTypes", canonicalSimpleName(canonicalType)),
                Optional.of(generatedV3Adapter(context)));
        }
        if (transport == PipelineTransport.LOCAL) {
            return new CompletionBoundary(domainType, Optional.empty());
        }

        CanonicalTransportBindingResolver resolver = new CanonicalTransportBindingResolver(context);
        Optional<CanonicalTransportTypeBinding> normalized = resolver.resolve(
            TypeMapping.canonical(domainType, canonicalType));
        if (normalized.isPresent()) {
            CanonicalTransportTypeBinding binding = normalized.orElseThrow();
            CanonicalRecordTransportRenderer renderer = new CanonicalRecordTransportRenderer(context, resolver);
            if (transport == PipelineTransport.REST) {
                renderer.ensureRest(binding);
                return new CompletionBoundary(binding.restDtoType(), Optional.of(binding.restMapperType()));
            }
            renderer.ensureGrpc(binding);
            return new CompletionBoundary(binding.grpcType(), Optional.of(binding.grpcMapperType()));
        }

        return new CompletionBoundary(domainType, Optional.empty());
    }

    private boolean generatedV3DomainType(TypeName type, GenerationContext context) {
        return context.v3GeneratedDomainTypes()
            && context.pipelineBasePackage() != null
            && !context.pipelineBasePackage().isBlank()
            && type instanceof ClassName domain
            && domain.packageName().equals(context.pipelineBasePackage() + ".domain");
    }

    private ClassName generatedV3Adapter(GenerationContext context) {
        return ClassName.get(context.pipelineBasePackage() + ".domain", "PipelineDomainProtoAdapters");
    }

    private String canonicalSimpleName(String canonicalType) {
        String normalized = canonicalType.startsWith("<") && canonicalType.endsWith(">")
            ? canonicalType.substring(1, canonicalType.length() - 1)
            : canonicalType;
        int separator = Math.max(normalized.lastIndexOf('.'), normalized.lastIndexOf('/'));
        return separator < 0 ? normalized : normalized.substring(separator + 1);
    }

    private TypeName clientStepType(TypeName domainType, PipelineTransport transport, String pipelineBasePackage) {
        if (transport == null || transport == PipelineTransport.LOCAL || !(domainType instanceof ClassName className)) {
            return domainType;
        }
        if (transport == PipelineTransport.REST) {
            return DtoTypeUtils.toDtoType(domainType);
        }
        String packageName = className.packageName();
        String basePackage = pipelineBasePackage;
        if (basePackage == null || basePackage.isBlank()) {
            basePackage = packageName.endsWith(".common.domain")
                ? packageName.substring(0, packageName.length() - ".common.domain".length())
                : packageName.endsWith(".domain")
                    ? packageName.substring(0, packageName.length() - ".domain".length())
                    : packageName;
        }
        return ClassName.get(basePackage + ".grpc", "PipelineTypes", className.simpleName());
    }

    private String stripService(String generatedName) {
        return generatedName.endsWith("Service")
            ? generatedName.substring(0, generatedName.length() - "Service".length())
            : generatedName;
    }

    private record CompletionBoundary(TypeName transportType, Optional<ClassName> adapter) {
    }
}
