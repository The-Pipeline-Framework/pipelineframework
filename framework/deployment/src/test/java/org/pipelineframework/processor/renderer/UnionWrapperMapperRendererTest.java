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
import org.pipelineframework.config.template.PipelineTemplateUnion;
import org.pipelineframework.config.template.PipelineTemplateUnionVariant;
import org.pipelineframework.processor.ir.DeploymentRole;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class UnionWrapperMapperRendererTest {

    @TempDir
    Path tempDir;

    @Test
    void rendersWrapperMapperThatComposesVariantMappersAndRejectsUnsetOneof() throws IOException {
        PipelineTemplateUnion union = new PipelineTemplateUnion(
            "PaymentOutcome",
            Map.of(
                "captured", new PipelineTemplateUnionVariant("captured", "PaymentCaptured", 1),
                "rejected", new PipelineTemplateUnionVariant("rejected", "PaymentRejected", 2),
                "requiresReview", new PipelineTemplateUnionVariant("requiresReview", "PaymentRequiresReview", 3)));
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        GenerationContext context = new GenerationContext(
            processingEnv,
            tempDir,
            DeploymentRole.PIPELINE_SERVER,
            Set.of(),
            null,
            null);

        new UnionWrapperMapperRenderer().render(
            "org.example.payment",
            union,
            ClassName.get("org.example.payment.common.domain", "PaymentOutcome"),
            context);

        Path sourcePath = tempDir.resolve("org/example/payment/pipeline/mapper/PaymentOutcomeUnionMapper.java");
        String source = Files.readString(sourcePath);

        assertTrue(source.contains("implements Mapper<PaymentOutcome, PipelineTypes.PaymentOutcome>"));
        assertTrue(source.contains("Mapper<PaymentCaptured, PipelineTypes.PaymentCaptured> capturedMapper;"));
        assertTrue(source.contains("Mapper<PaymentRejected, PipelineTypes.PaymentRejected> rejectedMapper;"));
        assertTrue(source.contains("Mapper<PaymentRequiresReview, PipelineTypes.PaymentRequiresReview> requiresReviewMapper;"));
        assertTrue(source.contains("case CAPTURED -> capturedMapper.fromExternal(external.getCaptured());"));
        assertTrue(source.contains("PaymentOutcome has no selected oneof variant"));
        assertTrue(source.contains("if (domain instanceof PaymentCaptured capturedValue)"));
        assertTrue(source.contains("setCaptured(capturedMapper.toExternal(capturedValue))"));
    }
}
