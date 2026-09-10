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

import static javax.tools.Diagnostic.Kind.ERROR;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AwaitStepTypeBindingResolverTest {

    @Test
    void rejectsCompletionConfigurationMissingItsTypeKey() {
        ProcessingEnvironment processing = mock(ProcessingEnvironment.class);
        Messager messager = mock(Messager.class);
        when(processing.getMessager()).thenReturn(messager);

        PipelineTemplateTypeModel typeModel = new PipelineTemplateTypeModel(Map.of(
            "Decision", new PipelineTemplateTypeDefinition.RecordType("Decision", List.of()),
            "Result", new PipelineTemplateTypeDefinition.RecordType("Result", List.of())));
        PipelineTemplateStep authoredStep = mock(PipelineTemplateStep.class);
        when(authoredStep.name()).thenReturn("Clarify");
        when(authoredStep.inputTypeName()).thenReturn("Decision");
        when(authoredStep.outputTypeName()).thenReturn("Result");
        when(authoredStep.accepts()).thenReturn(List.of());
        PipelineTemplateConfig config = mock(PipelineTemplateConfig.class);
        when(config.dialect()).thenReturn(PipelineTemplateDialect.V3);
        when(config.basePackage()).thenReturn("com.example.await");
        when(config.typeModel()).thenReturn(typeModel);
        when(config.steps()).thenReturn(List.of(authoredStep));

        StepDefinition step = mock(StepDefinition.class);
        when(step.name()).thenReturn("Clarify");
        when(step.inputType()).thenReturn(ClassName.get("com.example.await.domain", "Decision"));
        when(step.outputType()).thenReturn(ClassName.get("com.example.await.domain", "Result"));
        when(step.awaitConfig()).thenReturn(Map.of(
            "completion", Map.of("projector", "com.example.await.ClarificationProjector")));
        PipelineCompilationContext context = new PipelineCompilationContext(
            processing, mock(RoundEnvironment.class));
        context.setPipelineTemplateConfig(config);

        assertTrue(new AwaitStepTypeBindingResolver().resolve(context, step).isEmpty());
        verify(messager).printMessage(
            ERROR,
            "Await step 'Clarify' completion is missing required key 'type'.");
    }
}
