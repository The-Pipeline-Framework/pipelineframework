/*
 * Copyright (c) 2026 Mariano Barcia
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

package org.pipelineframework.processor.awaitable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateDialect;
import org.pipelineframework.config.template.PipelineTemplateStep;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeModel;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.StepDefinition;
import org.pipelineframework.processor.ir.DeferredCompletionDefinition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AwaitStepTypeBindingResolverTest {

    @Test
    void resolvesOperationAndFinalOutputsIndependently() {
        ProcessingEnvironment processing = mock(ProcessingEnvironment.class);
        Messager messager = mock(Messager.class);
        when(processing.getMessager()).thenReturn(messager);

        PipelineTemplateTypeModel typeModel = new PipelineTemplateTypeModel(Map.of(
            "PendingDecision", new PipelineTemplateTypeDefinition.RecordType("PendingDecision", List.of()),
            "Result", new PipelineTemplateTypeDefinition.RecordType("Result", List.of())));
        PipelineTemplateStep authoredStep = mock(PipelineTemplateStep.class);
        when(authoredStep.name()).thenReturn("Clarify");
        when(authoredStep.inputTypeName()).thenReturn("PendingDecision");
        when(authoredStep.outputTypeName()).thenReturn("Result");
        when(authoredStep.accepts()).thenReturn(List.of());
        PipelineTemplateConfig config = mock(PipelineTemplateConfig.class);
        when(config.dialect()).thenReturn(PipelineTemplateDialect.V3);
        when(config.basePackage()).thenReturn("com.example.await");
        when(config.typeModel()).thenReturn(typeModel);
        when(config.steps()).thenReturn(List.of(authoredStep));

        StepDefinition step = mock(StepDefinition.class);
        when(step.name()).thenReturn("Clarify");
        when(step.outputType()).thenReturn(ClassName.get("com.example.await.domain", "Result"));
        when(step.deferredCompletion()).thenReturn(Optional.of(new DeferredCompletionDefinition(
            "PendingDecision", Optional.empty(), "PT5M", List.of(), "interactionId",
            "interaction-api", Map.of(), Optional.empty())));
        PipelineCompilationContext context = new PipelineCompilationContext(
            processing, mock(RoundEnvironment.class));
        context.setPipelineTemplateConfig(config);

        Optional<AwaitStepTypeBinding> binding = new AwaitStepTypeBindingResolver().resolve(context, step);

        assertTrue(binding.isPresent());
        assertEquals("com.example.await.domain.PendingDecision", binding.orElseThrow().operationOutputType().toString());
        assertEquals("com.example.await.domain.Result", binding.orElseThrow().finalOutputType().toString());
    }

    @Test
    void crossModuleResolutionUsesCanonicalOperationOutputInsteadOfPrivateServiceRepresentation() {
        ProcessingEnvironment processing = mock(ProcessingEnvironment.class);
        when(processing.getMessager()).thenReturn(mock(Messager.class));

        PipelineTemplateTypeModel typeModel = new PipelineTemplateTypeModel(Map.of(
            "PendingDecision", new PipelineTemplateTypeDefinition.RecordType("PendingDecision", List.of()),
            "Result", new PipelineTemplateTypeDefinition.RecordType("Result", List.of())));
        PipelineTemplateStep authoredStep = mock(PipelineTemplateStep.class);
        when(authoredStep.name()).thenReturn("Clarify");
        when(authoredStep.outputTypeName()).thenReturn("Result");
        PipelineTemplateConfig config = mock(PipelineTemplateConfig.class);
        when(config.dialect()).thenReturn(PipelineTemplateDialect.V3);
        when(config.basePackage()).thenReturn("com.example.await");
        when(config.typeModel()).thenReturn(typeModel);
        when(config.steps()).thenReturn(List.of(authoredStep));

        StepDefinition step = mock(StepDefinition.class);
        when(step.name()).thenReturn("Clarify");
        when(step.outputType()).thenReturn(ClassName.get("com.example.await.domain", "Result"));
        when(step.deferredCompletion()).thenReturn(Optional.of(new DeferredCompletionDefinition(
            "PendingDecision",
            Optional.of(ClassName.get("com.example.await.service", "PendingDecisionEntity")),
            "PT5M", List.of(), "interactionId", "interaction-api", Map.of(), Optional.empty())));
        PipelineCompilationContext context = new PipelineCompilationContext(
            processing, mock(RoundEnvironment.class));
        context.setPipelineTemplateConfig(config);

        Optional<AwaitStepTypeBinding> binding = new AwaitStepTypeBindingResolver()
            .resolveCanonicalBoundary(context, step);

        assertTrue(binding.isPresent());
        assertEquals("com.example.await.domain.PendingDecision",
            binding.orElseThrow().operationOutputType().toString());
    }
}
