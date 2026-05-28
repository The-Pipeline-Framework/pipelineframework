package org.pipelineframework.processor.util;

import java.util.Set;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetSystemProperty;
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
            "ProcessInvoiceApprovalService",
            "ProcessInvoiceApprovalService",
            StreamingShape.UNARY_UNARY,
            false,
            "InvoiceApproval",
            "InvoiceSettlement");

        assertEquals("/api/v1/invoice-settlement", RestPathResolver.resolveResourcePath(model, null));
        assertEquals("/", RestPathResolver.resolveOperationPath(null));
    }

    @Test
    void resourcefulStrategyUsesInputTypeForUnaryStreaming() {
        PipelineStepModel model = step(
            "ProcessInvoiceApprovalService",
            "ProcessInvoiceApprovalService",
            StreamingShape.UNARY_STREAMING,
            false,
            "InvoiceApproval",
            "InvoiceSettlement");

        assertEquals("/api/v1/invoice-approval", RestPathResolver.resolveResourcePath(model, null));
    }

    @Test
    void resourcefulStrategyAddsPluginSegmentForSideEffects() {
        PipelineStepModel model = step(
            "ObservePersistenceInvoiceApprovalSideEffectService",
            "PersistenceInvoiceApprovalSideEffect",
            StreamingShape.UNARY_UNARY,
            true,
            "InvoiceApproval",
            "InvoiceApproval");

        assertEquals("/api/v1/invoice-approval/persistence", RestPathResolver.resolveResourcePath(model, null));
    }

    @Test
    @SetSystemProperty(key = RestPathResolver.REST_NAMING_STRATEGY_OPTION, value = "LEGACY")
    void legacyStrategyPreservesProcessNamingConvention() {
        PipelineStepModel model = step(
            "ProcessInvoiceApprovalService",
            "ProcessInvoiceApprovalReactiveService",
            StreamingShape.UNARY_UNARY,
            false,
            "InvoiceApproval",
            "InvoiceSettlement");

        assertEquals("/api/v1/process-invoice-approval", RestPathResolver.resolveResourcePath(model, null));
        assertEquals("/process", RestPathResolver.resolveOperationPath(null));
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
