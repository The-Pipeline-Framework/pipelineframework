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
import java.util.Locale;
import java.util.concurrent.CompletionStage;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.RestBinding;
import org.pipelineframework.processor.ir.ServiceApiKind;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.util.DtoTypeUtils;
import org.pipelineframework.processor.util.ResourceNameUtils;
import org.pipelineframework.processor.util.RestPathResolver;

/**
 * Spring WebFlux renderer for unary REST client steps.
 */
public class SpringRestClientStepRenderer implements PipelineRenderer<RestBinding> {

    @Override
    public GenerationTarget target() {
        return GenerationTarget.REST_CLIENT_STEP;
    }

    @Override
    public void render(RestBinding binding, GenerationContext ctx) throws IOException {
        PipelineStepModel model = binding.model();
        validateSupported(model);

        TypeSpec clientStepClass = buildClientStepClass(binding, ctx);
        JavaFile.builder(model.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX, clientStepClass)
            .build()
            .writeTo(ctx.outputDir());
    }

    private TypeSpec buildClientStepClass(RestBinding binding, GenerationContext ctx) {
        PipelineStepModel model = binding.model();
        TypeName inputDomainType = model.inboundDomainType();
        TypeName outputDomainType = model.outboundDomainType();
        TypeName inputDtoType = DtoTypeUtils.toDtoType(inputDomainType);
        TypeName outputDtoType = DtoTypeUtils.toDtoType(outputDomainType);
        TypeName stepInterface = ParameterizedTypeName.get(
            ClassName.get("org.pipelineframework.runtime.core", "PipelineUnaryStep"),
            inputDomainType,
            outputDomainType);
        String endpointPath = endpointPath(binding, ctx);
        String configKey = "tpf.rest-client." + restClientName(model.serviceName()) + ".url";

        return TypeSpec.classBuilder(clientStepClassName(model))
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.springframework.stereotype", "Component")).build())
            .addSuperinterface(stepInterface)
            .addField(FieldSpec.builder(
                    ClassName.get("org.springframework.web.reactive.function.client", "WebClient"),
                    "webClient",
                    Modifier.PRIVATE,
                    Modifier.FINAL)
                .build())
            .addField(FieldSpec.builder(
                    String.class,
                    "endpointUrl",
                    Modifier.PRIVATE,
                    Modifier.FINAL)
                .build())
            .addField(FieldSpec.builder(
                    mapperType(inputDomainType, inputDtoType),
                    "inboundMapper",
                    Modifier.PRIVATE,
                    Modifier.FINAL)
                .build())
            .addField(FieldSpec.builder(
                    mapperType(outputDomainType, outputDtoType),
                    "outboundMapper",
                    Modifier.PRIVATE,
                    Modifier.FINAL)
                .build())
            .addMethod(constructor(inputDomainType, inputDtoType, outputDomainType, outputDtoType))
            .addMethod(applyMethod(inputDomainType, outputDomainType, inputDtoType, outputDtoType))
            .addMethod(resolveEndpointUrlMethod(configKey, endpointPath))
            .addMethod(normalizeBaseUrlMethod())
            .build();
    }

    private MethodSpec constructor(
            TypeName inputDomainType,
            TypeName inputDtoType,
            TypeName outputDomainType,
            TypeName outputDtoType) {
        return MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addParameter(ClassName.get("org.springframework.web.reactive.function.client", "WebClient", "Builder"),
                "webClientBuilder")
            .addParameter(ClassName.get("org.springframework.core.env", "Environment"), "environment")
            .addParameter(mapperType(inputDomainType, inputDtoType), "inboundMapper")
            .addParameter(mapperType(outputDomainType, outputDtoType), "outboundMapper")
            .addStatement("this.webClient = webClientBuilder.build()")
            .addStatement("this.endpointUrl = resolveEndpointUrl(environment)")
            .addStatement("this.inboundMapper = inboundMapper")
            .addStatement("this.outboundMapper = outboundMapper")
            .build();
    }

    private MethodSpec applyMethod(
            TypeName inputDomainType,
            TypeName outputDomainType,
            TypeName inputDtoType,
            TypeName outputDtoType) {
        TypeName completionStage = ParameterizedTypeName.get(ClassName.get(CompletionStage.class), outputDomainType);
        return MethodSpec.methodBuilder("apply")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(completionStage)
            .addParameter(inputDomainType, "input")
            .addStatement("$T inputDto = this.inboundMapper.toExternal(input)", inputDtoType)
            .addStatement("return this.webClient.post()\n"
                    + ".uri(this.endpointUrl)\n"
                    + ".bodyValue(inputDto)\n"
                    + ".retrieve()\n"
                    + ".bodyToMono($T.class)\n"
                    + ".switchIfEmpty($T.error(new $T($S)))\n"
                    + ".map(this.outboundMapper::fromExternal)\n"
                    + ".toFuture()",
                outputDtoType,
                ClassName.get("reactor.core.publisher", "Mono"),
                IllegalStateException.class,
                "REST client step received an empty response body")
            .build();
    }

    private String endpointPath(RestBinding binding, GenerationContext ctx) {
        return joinPaths(resolveServicePath(binding, ctx),
            normalizePath(RestPathResolver.resolveOperationPath(ctx.processingEnv()), "operation", binding.model()));
    }

    private String resolveServicePath(RestBinding binding, GenerationContext ctx) {
        String servicePath = binding.restPathOverride() != null
            ? binding.restPathOverride()
            : RestPathResolver.resolveResourcePath(binding.model(), ctx.processingEnv());
        return normalizePath(servicePath, "resource", binding.model());
    }

    private String joinPaths(String servicePath, String operationPath) {
        if ("/".equals(servicePath)) {
            return operationPath;
        }
        if ("/".equals(operationPath)) {
            return servicePath + "/";
        }
        return servicePath + operationPath;
    }

    private String normalizePath(String path, String label, PipelineStepModel model) {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException(
                "REST client step requires a non-blank " + label + " path; step '"
                    + model.serviceName() + "'");
        }
        String normalized = path.strip();
        if (normalized.contains("://") || normalized.contains("?") || normalized.contains("#")) {
            throw new IllegalArgumentException(
                "REST client step " + label + " path must be a path, not a full URL or query/fragment target; step '"
                    + model.serviceName() + "'");
        }
        if (!normalized.startsWith("/")) {
            normalized = "/" + normalized;
        }
        while (normalized.length() > 1 && normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private MethodSpec resolveEndpointUrlMethod(String configKey, String endpointPath) {
        return MethodSpec.methodBuilder("resolveEndpointUrl")
            .addModifiers(Modifier.PRIVATE)
            .returns(String.class)
            .addParameter(ClassName.get("org.springframework.core.env", "Environment"), "environment")
            .addStatement("String baseUrl = environment.getProperty($S)", configKey)
            .beginControlFlow("if (baseUrl == null || baseUrl.isBlank())")
            .addStatement("throw new $T($S)",
                IllegalStateException.class,
                "Missing required Spring REST client URL property '" + configKey + "'")
            .endControlFlow()
            .addStatement("return normalizeBaseUrl(baseUrl) + $S", endpointPath)
            .build();
    }

    private MethodSpec normalizeBaseUrlMethod() {
        return MethodSpec.methodBuilder("normalizeBaseUrl")
            .addModifiers(Modifier.PRIVATE)
            .returns(String.class)
            .addParameter(String.class, "baseUrl")
            .addStatement("String normalized = baseUrl.strip()")
            .beginControlFlow("while (normalized.endsWith($S))", "/")
            .addStatement("normalized = normalized.substring(0, normalized.length() - 1)")
            .endControlFlow()
            .addStatement("return normalized")
            .build();
    }

    private TypeName mapperType(TypeName domainType, TypeName dtoType) {
        return ParameterizedTypeName.get(ClassName.get("org.pipelineframework.mapper", "Mapper"), domainType, dtoType);
    }

    private void validateSupported(PipelineStepModel model) {
        if (model.streamingShape() != StreamingShape.UNARY_UNARY) {
            throw new IllegalArgumentException(
                "Spring renderer profile currently supports only unary-unary REST client steps; step '"
                    + model.serviceName() + "' has shape " + model.streamingShape());
        }
        if (model.inboundDomainType() == null || model.outboundDomainType() == null) {
            throw new IllegalArgumentException(
                "Unary-unary REST client steps require non-null input and output domain types; step '"
                    + model.serviceName() + "'");
        }
        if (model.serviceApiKind() != ServiceApiKind.REACTIVE
            && model.serviceApiKind() != ServiceApiKind.BLOCKING) {
            throw new IllegalArgumentException(
                "Spring renderer profile currently supports only reactive or blocking unary services; step '"
                    + model.serviceName() + "' has API kind " + model.serviceApiKind());
        }
        if (model.sideEffect()) {
            throw new IllegalArgumentException(
                "Spring renderer profile does not yet support side-effect REST client steps; step '"
                    + model.serviceName() + "'");
        }
        if (model.remoteExecution() != null || model.delegateService() != null) {
            throw new IllegalArgumentException(
                "Spring renderer profile currently supports only internal REST client steps; step '"
                    + model.serviceName() + "'");
        }
    }

    private String clientStepClassName(PipelineStepModel model) {
        return ResourceNameUtils.normalizeBaseName(model.generatedName()) + PipelineStepProcessor.REST_CLIENT_STEP_SUFFIX;
    }

    private static String restClientName(String serviceName) {
        if (serviceName == null || serviceName.isBlank()) {
            throw new IllegalArgumentException("Spring REST client step requires a non-blank service name");
        }
        String baseName = serviceName.replaceFirst("Service$", "");
        String withBoundaryHyphens = baseName
            .replaceAll("([A-Z]+)([A-Z][a-z])", "$1-$2")
            .replaceAll("([a-z0-9])([A-Z])", "$1-$2");
        return withBoundaryHyphens.toLowerCase(Locale.ROOT);
    }
}
