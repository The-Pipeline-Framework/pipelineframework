package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AwaitClientStepRendererTest {

    @TempDir
    Path tempDir;

    @Test
    void targetReturnsAwaitClientStepTarget() {
        assertEquals(GenerationTarget.AWAIT_CLIENT_STEP, new AwaitClientStepRenderer().target());
    }

    @Test
    void rendersReactiveStepThatDelegatesToAwaitSupport() throws IOException {
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("FraudCheck")
            .generatedName("FraudCheckService")
            .servicePackage("com.example.fraud")
            .serviceClassName(ClassName.get("org.pipelineframework.awaitable", "AwaitStepDescriptor"))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.DEFAULT)
            .inputMapping(new TypeMapping(ClassName.get("com.example.fraud", "FraudCheckRequest"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.fraud", "FraudCheckDecision"), null, false))
            .enabledTargets(Set.of(GenerationTarget.AWAIT_CLIENT_STEP))
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .build();

        new AwaitClientStepRenderer().render(model, generationContext("LOCAL"));

        String source = Files.readString(tempDir.resolve(
            "com/example/fraud/pipeline/FraudCheckAwaitClientStep.java"));

        assertTrue(source.contains("implements StepOneToOne<FraudCheckRequest, FraudCheckDecision>"));
        assertTrue(source.contains("AwaitStepSupport support"));
        assertTrue(source.contains("AwaitStepDescriptorFactory descriptorFactory"));
        assertTrue(source.contains("support.awaitOneToOne(descriptorFactory.descriptor(\"FraudCheck\", "
            + "\"com.example.fraud.FraudCheckRequest\", \"com.example.fraud.FraudCheckDecision\")"
            + ", input)"));
    }

    @Test
    void rendersManyToManyAwaitClientStep() throws IOException {
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("AwaitPaymentProvider")
            .generatedName("AwaitPaymentProviderService")
            .servicePackage("com.example.payment")
            .serviceClassName(ClassName.get("org.pipelineframework.awaitable", "AwaitStepDescriptor"))
            .streamingShape(StreamingShape.STREAMING_STREAMING)
            .executionMode(ExecutionMode.DEFAULT)
            .inputMapping(new TypeMapping(ClassName.get("com.example.payment", "PaymentRecord"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.payment", "PaymentStatus"), null, false))
            .enabledTargets(Set.of(GenerationTarget.AWAIT_CLIENT_STEP))
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .build();

        new AwaitClientStepRenderer().render(model, generationContext("GRPC"));

        String source = Files.readString(tempDir.resolve(
            "com/example/payment/pipeline/AwaitPaymentProviderAwaitClientStep.java"));

        assertTrue(source.contains("implements StepManyToMany"));
        assertTrue(source.contains("PipelineTypes.PaymentRecord"));
        assertTrue(source.contains("PipelineTypes.PaymentStatus"));
        assertTrue(source.contains("applyTransform("));
        assertTrue(source.contains("return support.awaitManyToMany(descriptorFactory.descriptor(\"AwaitPaymentProvider\", "
            + "\"com.example.payment.grpc.PipelineTypes.PaymentRecord\", "
            + "\"com.example.payment.grpc.PipelineTypes.PaymentStatus\")"
            + ", input)"));
    }

    private GenerationContext generationContext(String transport) {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(Map.of("pipeline.transport", transport));
        return new GenerationContext(
            processingEnv,
            tempDir,
            DeploymentRole.ORCHESTRATOR_CLIENT,
            Set.of(),
            null,
            null);
    }
}
