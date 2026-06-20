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
import org.pipelineframework.processor.ir.LocalBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.ReactiveReturnKind;
import org.pipelineframework.processor.ir.ServiceApiKind;
import org.pipelineframework.processor.ir.StreamingShape;

/**
 * Spring renderer for the first supported portability slice: local unary client steps.
 */
public class SpringLocalClientStepRenderer implements PipelineRenderer<LocalBinding> {

    @Override
    public GenerationTarget target() {
        return GenerationTarget.LOCAL_CLIENT_STEP;
    }

    @Override
    public void render(LocalBinding binding, GenerationContext ctx) throws IOException {
        PipelineStepModel model = binding.model();
        validateSupported(model);

        TypeSpec clientStepClass = buildClientStepClass(model);
        JavaFile.builder(model.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX, clientStepClass)
            .build()
            .writeTo(ctx.outputDir());
    }

    private TypeSpec buildClientStepClass(PipelineStepModel model) {
        TypeName inputType = resolveDomainType(model.inboundDomainType());
        TypeName outputType = resolveDomainType(model.outboundDomainType());
        TypeName stepInterface = ParameterizedTypeName.get(
            ClassName.get("org.pipelineframework.runtime.core", "PipelineUnaryStep"),
            inputType,
            outputType);
        TypeName completionStage = ParameterizedTypeName.get(ClassName.get(CompletionStage.class), outputType);
        TypeName serviceType = model.serviceClassName();
        String serviceFieldName = decapitalize(model.serviceClassName().simpleName());

        FieldSpec serviceField = FieldSpec.builder(serviceType, serviceFieldName, Modifier.PRIVATE, Modifier.FINAL)
            .build();
        MethodSpec constructor = MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addParameter(serviceType, serviceFieldName)
            .addStatement("this.$N = $N", serviceFieldName, serviceFieldName)
            .build();
        MethodSpec applyMethod = MethodSpec.methodBuilder("apply")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(completionStage)
            .addParameter(inputType, "input")
            .addStatement("return this.$N.process(input).$L", serviceFieldName, completionStageAdapter(model))
            .build();

        return TypeSpec.classBuilder(getClientStepClassName(model))
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(AnnotationSpec.builder(ClassName.get("org.springframework.stereotype", "Component")).build())
            .addSuperinterface(stepInterface)
            .addField(serviceField)
            .addMethod(constructor)
            .addMethod(applyMethod)
            .build();
    }

    private String completionStageAdapter(PipelineStepModel model) {
        return switch (model.reactiveReturnKind()) {
            case MUTINY_UNI -> "subscribeAsCompletionStage()";
            case REACTOR_MONO -> "toFuture()";
        };
    }

    private void validateSupported(PipelineStepModel model) {
        if (model.streamingShape() != StreamingShape.UNARY_UNARY) {
            throw new IllegalArgumentException(
                "Spring renderer profile currently supports only unary-unary LOCAL steps; step '"
                    + model.serviceName() + "' has shape " + model.streamingShape());
        }
        if (model.serviceApiKind() != ServiceApiKind.REACTIVE) {
            throw new IllegalArgumentException(
                "Spring renderer profile currently supports only reactive-authored services; step '"
                    + model.serviceName() + "' has API kind " + model.serviceApiKind());
        }
        if (model.sideEffect()) {
            throw new IllegalArgumentException(
                "Spring renderer profile does not yet support side-effect steps; step '" + model.serviceName() + "'");
        }
        if (model.delegateService() != null || model.remoteExecution() != null) {
            throw new IllegalArgumentException(
                "Spring renderer profile currently supports only internal local steps; step '" + model.serviceName() + "'");
        }
    }

    private String getClientStepClassName(PipelineStepModel model) {
        String serviceClassName = model.generatedName();
        if (serviceClassName.endsWith("Service")) {
            serviceClassName = serviceClassName.substring(0, serviceClassName.length() - "Service".length());
        }
        return serviceClassName + "LocalClientStep";
    }

    private TypeName resolveDomainType(TypeName type) {
        return type != null ? type : ClassName.OBJECT;
    }

    private String decapitalize(String simpleName) {
        if (simpleName == null || simpleName.isBlank()) {
            return "service";
        }
        if (simpleName.length() == 1) {
            return simpleName.toLowerCase();
        }
        return Character.toLowerCase(simpleName.charAt(0)) + simpleName.substring(1);
    }
}
