package org.pipelineframework.processor.util;

import java.util.Set;

import org.junit.jupiter.api.Test;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;

import com.squareup.javapoet.ClassName;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ClientStepClassNamesTest {

    @Test
    void suffixPrefersAwaitThenCommandThenQueryBeforeDefault() {
        PipelineStepModel awaitCommandQuery = model(
            "ReserveStockService",
            Set.of(
                GenerationTarget.AWAIT_CLIENT_STEP,
                GenerationTarget.COMMAND_CLIENT_STEP,
                GenerationTarget.QUERY_CLIENT_STEP));
        PipelineStepModel commandQuery = model(
            "ReserveStockService",
            Set.of(GenerationTarget.COMMAND_CLIENT_STEP, GenerationTarget.QUERY_CLIENT_STEP));
        PipelineStepModel queryOnly = model(
            "ReserveStockService",
            Set.of(GenerationTarget.QUERY_CLIENT_STEP));
        PipelineStepModel plainClient = model("ReserveStockService", Set.of(GenerationTarget.CLIENT_STEP));

        assertEquals("AwaitClientStep", ClientStepClassNames.suffix(awaitCommandQuery, "GrpcClientStep"));
        assertEquals("CommandClientStep", ClientStepClassNames.suffix(commandQuery, "GrpcClientStep"));
        assertEquals("QueryClientStep", ClientStepClassNames.suffix(queryOnly, "GrpcClientStep"));
        assertEquals("GrpcClientStep", ClientStepClassNames.suffix(plainClient, "GrpcClientStep"));
    }

    @Test
    void stripTrailingServiceHandlesNullBlankAndNamesWithoutServiceSuffix() {
        assertEquals("", ClientStepClassNames.stripTrailingService(null));
        assertEquals("", ClientStepClassNames.stripTrailingService(""));
        assertEquals("ReserveStock", ClientStepClassNames.stripTrailingService("ReserveStock"));
        assertEquals("ReserveStock", ClientStepClassNames.stripTrailingService("ReserveStockService"));
    }

    private static PipelineStepModel model(String generatedName, Set<GenerationTarget> enabledTargets) {
        return new PipelineStepModel.Builder()
            .serviceName("ReserveStock")
            .generatedName(generatedName)
            .servicePackage("com.example.order")
            .serviceClassName(ClassName.get("com.example.order", generatedName))
            .inputMapping(new TypeMapping(ClassName.get("com.example.common.domain", "Input"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.common.domain", "Output"), null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(enabledTargets)
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .build();
    }
}
