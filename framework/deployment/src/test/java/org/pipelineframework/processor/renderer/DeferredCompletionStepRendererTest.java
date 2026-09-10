package org.pipelineframework.processor.renderer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.DeferredCompletionSelection;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.PipelineTransport;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeferredCompletionStepRendererTest {
    @TempDir Path tempDir;

    @Test
    void rendersCompletionModifierWithDirectDescriptor() throws Exception {
        PipelineStepModel model = model(StreamingShape.UNARY_UNARY);

        new DeferredCompletionStepRenderer().render(model, context(PipelineTransport.LOCAL));

        String source = Files.readString(tempDir.resolve(
            "com/example/approval/pipeline/CreateApprovalDeferredCompletionStep.java"));
        assertTrue(!source.contains("CreateApprovalLocalClientStep operation"));
        assertTrue(source.contains("StepOneToOne<PendingApproval, ApprovalDecision>"));
        assertTrue(source.contains("completionSupport.<PendingApproval, ApprovalDecision>awaitOneToOne(completion, operationOutput)"));
        assertTrue(source.contains("descriptorRegistry.register(new AwaitCompletionDescriptor"));
        assertTrue(source.contains("\"interaction-api\""));
        assertTrue(source.contains("\"orderId\""));
        assertTrue(source.contains("@Startup"));
        assertTrue(source.contains("AwaitStreamOneToOneStep<PendingApproval, ApprovalDecision>"));
    }

    @Test
    void streamingOperationAwaitsEachEmittedResultWithoutAggregateAwait() throws Exception {
        PipelineStepModel model = model(StreamingShape.UNARY_STREAMING);

        new DeferredCompletionStepRenderer().render(model, context(PipelineTransport.GRPC));

        String source = Files.readString(tempDir.resolve(
            "com/example/approval/pipeline/CreateApprovalDeferredCompletionStep.java"));
        assertTrue(!source.contains("CreateApprovalGrpcClientStep operation"));
        assertTrue(source.contains("StepOneToOne<PipelineTypes.PendingApproval, PipelineTypes.ApprovalDecision>"));
        assertTrue(source.contains("awaitOneToOneStream(completion, input)"));
        assertTrue(!source.contains("awaitOneToMany"));
    }

    @Test
    void localGeneratedUnionCompletionUsesPinnedGrpcWireContract() throws Exception {
        PipelineStepModel model = model(StreamingShape.UNARY_STREAMING).toBuilder()
            .inputMapping(TypeMapping.withoutMapper(
                ClassName.get("com.example.approval.domain", "ApprovalRequest")))
            .outputMapping(TypeMapping.withoutMapper(
                ClassName.get("com.example.approval.domain", "PendingApproval")))
            .deferredCompletionSelection(new DeferredCompletionSelection(
                ClassName.get("com.example.approval.domain", "ApprovalDecision"),
                "com.example.approval.domain.ApprovalDecision", Optional.empty(), Duration.ofMinutes(30),
                List.of("orderId"), "interactionId", "kafka", Map.of(), Optional.empty(), Optional.empty()))
            .build();

        new DeferredCompletionStepRenderer().render(model, context(PipelineTransport.LOCAL));

        String source = Files.readString(tempDir.resolve(
            "com/example/approval/pipeline/CreateApprovalDeferredCompletionStep.java"));
        assertTrue(source.contains("StepOneToOne<PendingApproval, ApprovalDecision>"), source);
        assertTrue(source.contains("\"com.example.approval.grpc.PipelineTypes.PendingApproval\""), source);
        assertTrue(source.contains("\"com.example.approval.grpc.PipelineTypes.ApprovalDecision\""), source);
        assertTrue(source.contains("PipelineDomainProtoAdapters.toProto"), source);
        assertTrue(source.contains("PipelineDomainProtoAdapters.fromProto"), source);
    }

    @Test
    void localV3WithoutBasePackageDoesNotConstructGeneratedAdapter() throws Exception {
        PipelineStepModel model = model(StreamingShape.UNARY_UNARY).toBuilder()
            .inputMapping(TypeMapping.withoutMapper(
                ClassName.get("com.example.approval.domain", "ApprovalRequest")))
            .outputMapping(TypeMapping.withoutMapper(
                ClassName.get("com.example.approval.domain", "PendingApproval")))
            .build();
        GenerationContext context = new GenerationContext(
            null, tempDir, DeploymentRole.ORCHESTRATOR_CLIENT, Set.of(),
            null, null, PipelineTransport.LOCAL, "", null, true);

        new DeferredCompletionStepRenderer().render(model, context);

        String source = Files.readString(tempDir.resolve(
            "com/example/approval/pipeline/CreateApprovalDeferredCompletionStep.java"));
        assertTrue(source.contains("StepOneToOne<PendingApproval, ApprovalDecision>"), source);
        assertTrue(!source.contains("PipelineDomainProtoAdapters"), source);
    }

    @Test
    void exposesDeferredCompletionTarget() {
        assertEquals(GenerationTarget.DEFERRED_COMPLETION_STEP, new DeferredCompletionStepRenderer().target());
    }

    private PipelineStepModel model(StreamingShape shape) {
        return new PipelineStepModel.Builder()
            .serviceName("CreateApproval")
            .generatedName("CreateApprovalService")
            .servicePackage("com.example.approval")
            .serviceClassName(ClassName.get("com.example.approval", "CreateApprovalService"))
            .streamingShape(shape)
            .executionMode(ExecutionMode.DEFAULT)
            .inputMapping(TypeMapping.withoutMapper(ClassName.get("com.example.approval", "ApprovalRequest")))
            .outputMapping(TypeMapping.withoutMapper(ClassName.get("com.example.approval", "PendingApproval")))
            .enabledTargets(Set.of(GenerationTarget.DEFERRED_COMPLETION_STEP))
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .deferredCompletionSelection(new DeferredCompletionSelection(
                ClassName.get("com.example.approval", "ApprovalDecision"), "ApprovalDecision", Optional.empty(),
                Duration.ofMinutes(30),
                List.of("orderId"), "interactionId", "interaction-api", Map.of(),
                Optional.empty(), Optional.empty()))
            .build();
    }

    private GenerationContext context(PipelineTransport transport) {
        return new GenerationContext(null, tempDir, DeploymentRole.ORCHESTRATOR_CLIENT, Set.of(),
            null, null, transport, "com.example.approval", null, true);
    }
}
