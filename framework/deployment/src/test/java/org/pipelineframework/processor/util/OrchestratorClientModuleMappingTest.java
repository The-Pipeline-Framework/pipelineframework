package org.pipelineframework.processor.util;

import java.util.List;
import java.util.Properties;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class OrchestratorClientModuleMappingTest {

    @Test
    void resolvesFinalizeMergeStepFromExplicitProcessClientToken() {
        Properties properties = new Properties();
        properties.setProperty(
            "pipeline.module.payment-status-svc.steps",
            "process-approved-payment-status,process-unapproved-payment-status,process-finalize-payment-output");

        OrchestratorClientModuleMapping mapping = OrchestratorClientModuleMapping.fromProperties(properties, null)
            .withResolvedModules(List.of("input-csv-file-processing-svc", "orchestrator-svc", "payment-status-svc"));

        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("ProcessFinalizePaymentOutputService")
            .servicePackage("org.example")
            .serviceClassName(ClassName.get("org.example", "ProcessFinalizePaymentOutputService"))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .sideEffect(false)
            .build();

        assertEquals("payment-status-svc", mapping.resolveModuleName(model));

        OrchestratorClientModuleMapping.ClientConfig client = mapping.clientConfig(model);
        assertNotNull(client);
        assertEquals("process-finalize-payment-output", client.name());
        assertEquals("8446", client.port());
    }
}
