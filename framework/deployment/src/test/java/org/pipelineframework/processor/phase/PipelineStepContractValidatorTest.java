package org.pipelineframework.processor.phase;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.template.PipelinePlatform;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateMaterialization;
import org.pipelineframework.config.template.PipelineTemplateMessage;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.*;
import org.pipelineframework.processor.routing.PipelineBranchingPlan;

import static javax.tools.Diagnostic.Kind.ERROR;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class PipelineStepContractValidatorTest {

    @Test
    void rejectsCallbackOccurrencesInAStreamAndAcceptsAnExplicitAggregate() {
        ProcessingEnvironment processing = mock(ProcessingEnvironment.class);
        Messager messager = mock(Messager.class);
        when(processing.getMessager()).thenReturn(messager);
        var context = context(processing);
        var completion = mock(DeferredCompletionSelection.class);
        when(completion.callback()).thenReturn(java.util.Optional.of(
            mock(DeferredCompletionSelection.ResolvedConnectorCallback.class)));
        var callback = model("Callback", "Shared", "Final").toBuilder()
            .deferredCompletionSelection(completion).build();
        var expand = model("Expand", "Input", "Shared").toBuilder()
            .streamingShape(StreamingShape.UNARY_STREAMING).build();
        var ordinary = model("Transform", "Shared", "Shared");
        new PipelineStepContractValidator().validate(context, List.of(expand, ordinary, callback));
        verify(messager).printMessage(eq(ERROR), contains("cannot consume an upstream stream"));

        clearInvocations(messager);
        var aggregate = model("Aggregate", "Shared", "Shared").toBuilder()
            .streamingShape(StreamingShape.STREAMING_UNARY).build();
        new PipelineStepContractValidator().validate(context, List.of(expand, aggregate, callback));
        verifyNoInteractions(messager);
    }

    @Test
    void aggregationOnOneBranchDoesNotHideStreamingCallbackInputOnAnother() {
        ProcessingEnvironment processing = mock(ProcessingEnvironment.class);
        Messager messager = mock(Messager.class);
        when(processing.getMessager()).thenReturn(messager);
        var context = context(processing);
        var completion = mock(DeferredCompletionSelection.class);
        when(completion.callback()).thenReturn(java.util.Optional.of(
            mock(DeferredCompletionSelection.ResolvedConnectorCallback.class)));
        var callback = model("CallbackB", "B", "Final").toBuilder()
            .deferredCompletionSelection(completion).build();
        var expand = model("Expand", "Input", "Union").toBuilder()
            .streamingShape(StreamingShape.UNARY_STREAMING).build();
        var aggregate = model("AggregateA", "A", "C").toBuilder()
            .streamingShape(StreamingShape.STREAMING_UNARY).build();
        var plan = mock(PipelineBranchingPlan.class);
        when(plan.branchAware()).thenReturn(true);
        var steps = List.of(branch(0, "Input", List.of("A", "B")),
            branch(1, "A", List.of("C")), branch(2, "B", List.of("Final")));
        when(plan.steps()).thenReturn(steps);
        context.setBranchingPlan(plan);
        new PipelineStepContractValidator().validate(context, List.of(expand, aggregate, callback));
        verify(messager).printMessage(eq(ERROR), contains("cannot consume an upstream stream"));
    }

    private PipelineBranchingPlan.BranchStep branch(int index, String accepted, List<String> produced) {
        var step = mock(PipelineBranchingPlan.BranchStep.class);
        when(step.index()).thenReturn(index);
        when(step.acceptedContractTypes()).thenReturn(List.of(accepted));
        when(step.producedLeafContractTypes()).thenReturn(produced);
        return step;
    }

    @Test
    void reportsAdjacentResolvedJavaContractMismatch() {
        ProcessingEnvironment processing = mock(ProcessingEnvironment.class);
        Messager messager = mock(Messager.class);
        when(processing.getMessager()).thenReturn(messager);
        PipelineCompilationContext context = context(processing);
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
        PipelineCompilationContext context = context(processing);
        context.setPipelineTemplateConfig(configWithInputContract());

        new PipelineStepContractValidator().validate(context, List.of(
            model("First", "Input", "Shared"),
            model("Second", "Shared", "FinalOutput")));

        verify(messager, never()).printMessage(eq(ERROR), org.mockito.ArgumentMatchers.anyString());
    }

    @Test
    void defersNonAdjacentCompatibilityToAuthoritativeBranchPlan() {
        ProcessingEnvironment processing = mock(ProcessingEnvironment.class);
        Messager messager = mock(Messager.class);
        when(processing.getMessager()).thenReturn(messager);
        PipelineCompilationContext context = context(processing);
        context.setPipelineTemplateConfig(configWithInputContract());
        PipelineBranchingPlan branchPlan = mock(PipelineBranchingPlan.class);
        when(branchPlan.branchAware()).thenReturn(true);
        context.setBranchingPlan(branchPlan);

        new PipelineStepContractValidator().validate(context, List.of(
            model("First", "Input", "Union"),
            model("Narrowed", "AcceptedVariant", "FinalOutput")));

        verify(messager, never()).printMessage(eq(ERROR), org.mockito.ArgumentMatchers.anyString());
    }

    private PipelineCompilationContext context(ProcessingEnvironment processing) {
        return new PipelineCompilationContext(processing, mock(RoundEnvironment.class));
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
