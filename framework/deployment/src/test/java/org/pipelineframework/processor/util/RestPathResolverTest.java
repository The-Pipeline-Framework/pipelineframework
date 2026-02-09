package org.pipelineframework.processor.util;

import java.util.Set;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RestPathResolverTest {

    @Test
    void resourcefulStrategyUsesOutputTypeForUnaryUnary() {
        PipelineStepModel model = step(
            "ProcessAckPaymentSentService",
            "ProcessAckPaymentSentService",
            StreamingShape.UNARY_UNARY,
            false,
            "AckPaymentSent",
            "PaymentStatus");

        assertEquals("/api/v1/payment-status", RestPathResolver.resolveResourcePath(model, null));
        assertEquals("/", RestPathResolver.resolveOperationPath(null));
    }

    @Test
    void resourcefulStrategyUsesInputTypeForUnaryStreaming() {
        PipelineStepModel model = step(
            "ProcessAckPaymentSentService",
            "ProcessAckPaymentSentService",
            StreamingShape.UNARY_STREAMING,
            false,
            "AckPaymentSent",
            "PaymentStatus");

        assertEquals("/api/v1/ack-payment-sent", RestPathResolver.resolveResourcePath(model, null));
    }

    @Test
    void resourcefulStrategyAddsPluginSegmentForSideEffects() {
        PipelineStepModel model = step(
            "ObservePersistenceAckPaymentSentSideEffectService",
            "PersistenceAckPaymentSentSideEffect",
            StreamingShape.UNARY_UNARY,
            true,
            "AckPaymentSent",
            "AckPaymentSent");

        assertEquals("/api/v1/ack-payment-sent/persistence", RestPathResolver.resolveResourcePath(model, null));
    }

    @Test
    void legacyStrategyPreservesProcessNamingConvention() {
        String previous = System.getProperty(RestPathResolver.REST_NAMING_STRATEGY_OPTION);
        try {
            System.setProperty(RestPathResolver.REST_NAMING_STRATEGY_OPTION, "LEGACY");
            PipelineStepModel model = step(
                "ProcessAckPaymentSentService",
                "ProcessAckPaymentSentReactiveService",
                StreamingShape.UNARY_UNARY,
                false,
                "AckPaymentSent",
                "PaymentStatus");

            assertEquals("/api/v1/process-ack-payment-sent", RestPathResolver.resolveResourcePath(model, null));
            assertEquals("/process", RestPathResolver.resolveOperationPath(null));
        } finally {
            if (previous == null) {
                System.clearProperty(RestPathResolver.REST_NAMING_STRATEGY_OPTION);
            } else {
                System.setProperty(RestPathResolver.REST_NAMING_STRATEGY_OPTION, previous);
            }
        }
    }

    private PipelineStepModel step(
        String serviceName,
        String generatedName,
        StreamingShape shape,
        boolean sideEffect,
        String inboundType,
        String outboundType) {
        String servicePackage = "org.pipelineframework.csv.service";
        return new PipelineStepModel.Builder()
            .serviceName(serviceName)
            .generatedName(generatedName)
            .servicePackage(servicePackage)
            .serviceClassName(ClassName.get(servicePackage, serviceName))
            .streamingShape(shape)
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .sideEffect(sideEffect)
            .enabledTargets(Set.of(GenerationTarget.REST_RESOURCE))
            .inputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", inboundType),
                ClassName.get("org.pipelineframework.csv.common.mapper", inboundType + "Mapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", outboundType),
                ClassName.get("org.pipelineframework.csv.common.mapper", outboundType + "Mapper"),
                true))
            .build();
    }
}
