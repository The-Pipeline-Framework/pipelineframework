/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.util.Optional;
import javax.annotation.processing.Messager;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.GrpcBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.util.GrpcJavaTypeResolver;

/**
 * Generates the concrete service implementation for v2 remote operator steps.
 */
public final class RemoteOperatorAdapterRenderer implements PipelineRenderer<GrpcBinding> {
    private static final GrpcJavaTypeResolver GRPC_TYPE_RESOLVER = new GrpcJavaTypeResolver();
    private static final ClassName APPLICATION_SCOPED = ClassName.get("jakarta.enterprise.context", "ApplicationScoped");
    private static final ClassName INJECT = ClassName.get("jakarta.inject", "Inject");
    private static final ClassName POST_CONSTRUCT = ClassName.get("jakarta.annotation", "PostConstruct");
    private static final ClassName CONFIG_PROPERTY = ClassName.get("org.eclipse.microprofile.config.inject", "ConfigProperty");
    private static final ClassName OPTIONAL = ClassName.get(Optional.class);
    private static final ClassName MAPPER = ClassName.get("org.pipelineframework.mapper", "Mapper");
    private static final ClassName REACTIVE_SERVICE = ClassName.get("org.pipelineframework.service", "ReactiveService");
    private static final ClassName REMOTE_CLIENT = ClassName.get("org.pipelineframework.transport.http", "ProtobufHttpRemoteOperatorClient");

    @Override
    public GenerationTarget target() {
        return GenerationTarget.REMOTE_OPERATOR_ADAPTER;
    }

    @Override
    public void render(GrpcBinding binding, GenerationContext ctx) throws IOException {
        Messager messager = ctx.processingEnv() == null ? null : ctx.processingEnv().getMessager();
        TypeSpec typeSpec = buildRemoteAdapter(binding, messager, ctx.role());
        JavaFile.builder(
                binding.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
                typeSpec)
            .build()
            .writeTo(ctx.outputDir());
    }

    private TypeSpec buildRemoteAdapter(
        GrpcBinding binding,
        Messager messager,
        org.pipelineframework.processor.ir.DeploymentRole role
    ) {
        PipelineStepModel model = binding.model();
        if (model.remoteExecution() == null || !model.remoteExecution().isRemote()) {
            throw new IllegalStateException("Remote operator adapter rendering requires remote execution metadata");
        }

        GrpcJavaTypeResolver.GrpcJavaTypes grpcTypes = GRPC_TYPE_RESOLVER.resolve(binding, messager);
        TypeName domainInputType = model.inboundDomainType() != null ? model.inboundDomainType() : ClassName.OBJECT;
        TypeName domainOutputType = model.outboundDomainType() != null ? model.outboundDomainType() : ClassName.OBJECT;
        TypeName protoInputType = grpcTypes.grpcParameterType() != null ? grpcTypes.grpcParameterType() : ClassName.OBJECT;
        TypeName protoOutputType = grpcTypes.grpcReturnType() != null ? grpcTypes.grpcReturnType() : ClassName.OBJECT;

        TypeSpec.Builder builder = TypeSpec.classBuilder(model.serviceClassName().simpleName())
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(AnnotationSpec.builder(APPLICATION_SCOPED).build())
            .addAnnotation(AnnotationSpec.builder(Unremovable.class).build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.pipelineframework.annotation", "GeneratedRole"))
                .addMember("value", "$T.$L",
                    ClassName.get("org.pipelineframework.annotation", "GeneratedRole", "Role"),
                    role.name())
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.pipelineframework.annotation", "ParallelismHint"))
                .addMember("ordering", "$T.$L",
                    ClassName.get("org.pipelineframework.parallelism", "OrderingRequirement"),
                    model.orderingRequirement().name())
                .addMember("threadSafety", "$T.$L",
                    ClassName.get("org.pipelineframework.parallelism", "ThreadSafety"),
                    model.threadSafety().name())
                .build())
            .addSuperinterface(ParameterizedTypeName.get(REACTIVE_SERVICE, domainInputType, domainOutputType));

        builder.addField(FieldSpec.builder(REMOTE_CLIENT, "remoteOperatorClient")
            .addAnnotation(INJECT)
            .addModifiers(Modifier.PRIVATE)
            .build());
        builder.addField(FieldSpec.builder(
                ParameterizedTypeName.get(MAPPER, domainInputType, protoInputType),
                "requestMapper")
            .addAnnotation(INJECT)
            .addModifiers(Modifier.PRIVATE)
            .build());
        builder.addField(FieldSpec.builder(
                ParameterizedTypeName.get(MAPPER, domainOutputType, protoOutputType),
                "responseMapper")
            .addAnnotation(INJECT)
            .addModifiers(Modifier.PRIVATE)
            .build());

        if (model.remoteExecution().target() != null && model.remoteExecution().target().urlConfigKey() != null) {
            builder.addField(FieldSpec.builder(
                    ParameterizedTypeName.get(OPTIONAL, ClassName.get(String.class)),
                    "configuredTargetUrl")
                .addAnnotation(AnnotationSpec.builder(CONFIG_PROPERTY)
                    .addMember("name", "$S", model.remoteExecution().target().urlConfigKey())
                    .build())
                .addModifiers(Modifier.PRIVATE)
                .initializer("$T.empty()", OPTIONAL)
                .build());
        }

        builder.addMethod(MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .build());
        builder.addMethod(buildValidateTargetMethod(model));
        builder.addMethod(buildResolveTargetUrlMethod(model));
        builder.addMethod(buildDecodeResponseMethod(protoOutputType));
        builder.addMethod(buildProcessMethod(model, protoInputType));

        return builder.build();
    }

    private MethodSpec buildValidateTargetMethod(PipelineStepModel model) {
        return MethodSpec.methodBuilder("validateRemoteTarget")
            .addAnnotation(POST_CONSTRUCT)
            .addModifiers(Modifier.PUBLIC)
            .addStatement("resolveTargetUrl()")
            .addComment("Fail fast at startup when execution.target.urlConfigKey does not resolve.")
            .build();
    }

    private MethodSpec buildResolveTargetUrlMethod(PipelineStepModel model) {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("resolveTargetUrl")
            .addModifiers(Modifier.PRIVATE)
            .returns(String.class);
        String literalUrl = model.remoteExecution().target() == null ? null : model.remoteExecution().target().url();
        String urlConfigKey = model.remoteExecution().target() == null ? null : model.remoteExecution().target().urlConfigKey();
        if (literalUrl != null) {
            builder.addStatement("return $S", literalUrl);
            return builder.build();
        }
        builder.addStatement("String resolved = configuredTargetUrl.orElse(null)");
        builder.beginControlFlow("if (resolved == null || resolved.isBlank())")
            .addStatement("throw new IllegalStateException($S)",
                "Remote step '" + model.serviceName()
                    + "' requires config key '" + urlConfigKey + "' to resolve to a non-blank URL")
            .endControlFlow();
        builder.addStatement("return resolved.strip()");
        return builder.build();
    }

    private MethodSpec buildDecodeResponseMethod(TypeName protoOutputType) {
        return MethodSpec.methodBuilder("decodeResponse")
            .addModifiers(Modifier.PRIVATE)
            .returns(protoOutputType)
            .addParameter(byte[].class, "bytes")
            .beginControlFlow("try")
            .addStatement("return $T.parseFrom(bytes)", protoOutputType)
            .nextControlFlow("catch ($T e)", ClassName.get("com.google.protobuf", "InvalidProtocolBufferException"))
            .addStatement("throw new IllegalStateException($S, e)", "Failed to decode remote operator protobuf response")
            .endControlFlow()
            .build();
    }

    private MethodSpec buildProcessMethod(PipelineStepModel model, TypeName protoInputType) {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("process")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(ClassName.get(Uni.class), model.outboundDomainType()))
            .addParameter(model.inboundDomainType(), "input");

        if (model.executionMode() == org.pipelineframework.processor.ir.ExecutionMode.VIRTUAL_THREADS) {
            builder.addAnnotation(ClassName.get("io.smallrye.common.annotation", "RunOnVirtualThread"));
        }

        if (model.outboundDomainType() == null || model.inboundDomainType() == null) {
            throw new IllegalStateException("Remote operator adapter requires domain input and output types");
        }
        if (model.remoteExecution() == null || model.remoteExecution().operatorId() == null) {
            throw new IllegalStateException("Remote operator adapter requires a non-null remote execution operatorId");
        }
        Integer timeoutMs = model.remoteExecution().timeoutMs();

        builder.addStatement("$T request = requestMapper.toExternal(input)", protoInputType)
            .addStatement("return remoteOperatorClient.invoke(resolveTargetUrl(), $S, request.toByteArray(), $L)"
                    + ".map(bytes -> responseMapper.fromExternal(decodeResponse(bytes)))",
                model.remoteExecution().operatorId(),
                timeoutMs == null ? "null" : timeoutMs.toString());
        return builder.build();
    }
}
