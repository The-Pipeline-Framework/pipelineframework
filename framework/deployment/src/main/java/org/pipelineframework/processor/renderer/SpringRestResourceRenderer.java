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
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
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
 * Spring WebFlux renderer for the narrow REST unary portability smoke.
 */
public class SpringRestResourceRenderer implements PipelineRenderer<RestBinding> {

    @Override
    public GenerationTarget target() {
        return GenerationTarget.REST_RESOURCE;
    }

    @Override
    public void render(RestBinding binding, GenerationContext ctx) throws IOException {
        PipelineStepModel model = binding.model();
        validateSupported(model);

        TypeSpec resourceClass = buildResourceClass(binding, ctx);
        JavaFile.builder(model.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX, resourceClass)
            .build()
            .writeTo(ctx.outputDir());
    }

    private TypeSpec buildResourceClass(RestBinding binding, GenerationContext ctx) {
        PipelineStepModel model = binding.model();
        TypeName inputDomainType = resolveDomainType(model.inboundDomainType());
        TypeName outputDomainType = resolveDomainType(model.outboundDomainType());
        TypeName inputDtoType = DtoTypeUtils.toDtoType(inputDomainType);
        TypeName outputDtoType = DtoTypeUtils.toDtoType(outputDomainType);
        String servicePath = binding.restPathOverride() != null
            ? binding.restPathOverride()
            : RestPathResolver.resolveResourcePath(model, ctx.processingEnv());

        FieldSpec runnerField = FieldSpec.builder(
                ClassName.get("org.pipelineframework.runtime.spring", "SpringPipelineRunner"),
                "pipelineRunner",
                Modifier.PRIVATE,
                Modifier.FINAL)
            .build();
        FieldSpec inboundMapperField = FieldSpec.builder(
                mapperType(inputDomainType, inputDtoType),
                "inboundMapper",
                Modifier.PRIVATE,
                Modifier.FINAL)
            .build();
        FieldSpec outboundMapperField = FieldSpec.builder(
                mapperType(outputDomainType, outputDtoType),
                "outboundMapper",
                Modifier.PRIVATE,
                Modifier.FINAL)
            .build();

        return TypeSpec.classBuilder(resourceClassName(model))
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.springframework.web.bind.annotation", "RestController"))
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.springframework.web.bind.annotation", "RequestMapping"))
                .addMember("value", "$S", servicePath)
                .build())
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.pipelineframework.annotation", "GeneratedRole"))
                .addMember("value", "$T.$L",
                    ClassName.get("org.pipelineframework.annotation", "GeneratedRole", "Role"),
                    ctx.role().name())
                .build())
            .addField(runnerField)
            .addField(inboundMapperField)
            .addField(outboundMapperField)
            .addMethod(constructor(inputDomainType, inputDtoType, outputDomainType, outputDtoType))
            .addMethod(processMethod(ctx, inputDomainType, outputDomainType, inputDtoType, outputDtoType))
            .build();
    }

    private MethodSpec constructor(
            TypeName inputDomainType,
            TypeName inputDtoType,
            TypeName outputDomainType,
            TypeName outputDtoType) {
        TypeName runnerType = ClassName.get("org.pipelineframework.runtime.spring", "SpringPipelineRunner");
        MethodSpec.Builder builder = MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addParameter(runnerType, "pipelineRunner")
            .addParameter(mapperType(inputDomainType, inputDtoType), "inboundMapper")
            .addParameter(mapperType(outputDomainType, outputDtoType), "outboundMapper")
            .addStatement("this.pipelineRunner = pipelineRunner")
            .addStatement("this.inboundMapper = inboundMapper")
            .addStatement("this.outboundMapper = outboundMapper");
        return builder.build();
    }

    private MethodSpec processMethod(
            GenerationContext ctx,
            TypeName inputDomainType,
            TypeName outputDomainType,
            TypeName inputDtoType,
            TypeName outputDtoType) {
        TypeName monoOutput = ParameterizedTypeName.get(ClassName.get("reactor.core.publisher", "Mono"), outputDtoType);
        String operationPath = RestPathResolver.resolveOperationPath(ctx.processingEnv());

        return MethodSpec.methodBuilder("process")
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.springframework.web.bind.annotation", "PostMapping"))
                .addMember("value", "$S", operationPath)
                .build())
            .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class)
                .addMember("value", "$S", "unchecked")
                .build())
            .addModifiers(Modifier.PUBLIC)
            .returns(monoOutput)
            .addParameter(ParameterSpec.builder(inputDtoType, "inputDto")
                .addAnnotation(ClassName.get("org.springframework.web.bind.annotation", "RequestBody"))
                .build())
            .addStatement("$T inputDomain = this.inboundMapper.fromExternal(inputDto)", inputDomainType)
            .addStatement("return $T.fromCompletionStage(this.pipelineRunner.run(inputDomain))\n"
                    + ".map(output -> this.outboundMapper.toExternal(($T) output))",
                ClassName.get("reactor.core.publisher", "Mono"),
                outputDomainType)
            .build();
    }

    private TypeName mapperType(TypeName domainType, TypeName dtoType) {
        return ParameterizedTypeName.get(ClassName.get("org.pipelineframework.mapper", "Mapper"), domainType, dtoType);
    }

    private void validateSupported(PipelineStepModel model) {
        if (model.streamingShape() != StreamingShape.UNARY_UNARY) {
            throw new IllegalArgumentException(
                "Spring renderer profile currently supports only unary-unary REST resources; step '"
                    + model.serviceName() + "' has shape " + model.streamingShape());
        }
        if (model.inboundDomainType() == null || model.outboundDomainType() == null) {
            throw new IllegalArgumentException(
                "Unary-unary REST resources require non-null input and output domain types; step '"
                    + model.serviceName() + "'");
        }
        if (model.serviceApiKind() != ServiceApiKind.REACTIVE) {
            throw new IllegalArgumentException(
                "Spring renderer profile currently supports only reactive-authored services; step '"
                    + model.serviceName() + "' has API kind " + model.serviceApiKind());
        }
        if (model.sideEffect()) {
            throw new IllegalArgumentException(
                "Spring renderer profile does not yet support side-effect REST resources; step '" + model.serviceName() + "'");
        }
        if (model.delegateService() != null || model.remoteExecution() != null) {
            throw new IllegalArgumentException(
                "Spring renderer profile currently supports only internal REST resources; step '" + model.serviceName() + "'");
        }
    }

    private String resourceClassName(PipelineStepModel model) {
        return ResourceNameUtils.normalizeBaseName(model.generatedName()) + PipelineStepProcessor.REST_RESOURCE_SUFFIX;
    }

    private TypeName resolveDomainType(TypeName type) {
        return type != null ? type : ClassName.OBJECT;
    }
}
