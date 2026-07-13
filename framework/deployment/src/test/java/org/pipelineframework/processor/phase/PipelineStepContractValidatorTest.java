package org.pipelineframework.processor.phase;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.template.PipelinePlatform;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateMaterialization;
import org.pipelineframework.config.template.PipelineTemplateMessage;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.*;

import static javax.tools.Diagnostic.Kind.ERROR;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class PipelineStepContractValidatorTest {

    @Test
    void reportsAdjacentResolvedJavaContractMismatch() {
        ProcessingEnvironment processing = mock(ProcessingEnvironment.class);
        Messager messager = mock(Messager.class);
        when(processing.getMessager()).thenReturn(messager);
        PipelineCompilationContext context = new PipelineCompilationContext(processing, null);
        context.setPipelineTemplateConfig(configWithInputContract());

        new PipelineStepContractValidator().validate(context, List.of(
            model("First", "Input", "FirstOutput"),
            model("Second", "DifferentInput", "FinalOutput")));

        verify(messager).printMessage(eq(ERROR), contains("resolves Java input 'org.example.DifferentInput'"));
    }

    @Test
    void acceptsMatchingResolvedJavaContracts() {
        ProcessingEnvironment processing = mock(ProcessingEnvironment.class);
        Messager messager = mock(Messager.class);
        when(processing.getMessager()).thenReturn(messager);
        PipelineCompilationContext context = new PipelineCompilationContext(processing, null);
        context.setPipelineTemplateConfig(configWithInputContract());

        new PipelineStepContractValidator().validate(context, List.of(
            model("First", "Input", "Shared"),
            model("Second", "Shared", "FinalOutput")));

        verify(messager, never()).printMessage(eq(ERROR), org.mockito.ArgumentMatchers.anyString());
    }

    private PipelineTemplateConfig configWithInputContract() {
        PipelineTemplateMessage input = new PipelineTemplateMessage("Input", List.of(), null);
        return new PipelineTemplateConfig(
            2,
            "Contract Test",
            "org.example",
            "LOCAL",
            PipelinePlatform.COMPUTE,
            Map.of("Input", input),
            Map.of(),
            Map.of(),
            Map.of(),
            List.of(),
            Map.of(),
            null,
            null,
            new PipelineTemplateMaterialization(List.of()),
            "Input",
            null);
    }

    private PipelineStepModel model(String serviceName, String input, String output) {
        return new PipelineStepModel.Builder()
            .serviceName(serviceName)
            .generatedName(serviceName)
            .servicePackage("org.example")
            .serviceClassName(ClassName.get("org.example", serviceName))
            .inputMapping(new TypeMapping(ClassName.get("org.example", input), null, false))
            .outputMapping(new TypeMapping(ClassName.get("org.example", output), null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(Set.of())
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .build();
    }
}
