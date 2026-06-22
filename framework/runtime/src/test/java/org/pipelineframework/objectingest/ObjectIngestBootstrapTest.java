package org.pipelineframework.objectingest;

import jakarta.enterprise.inject.Instance;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class ObjectIngestBootstrapTest {

    @AfterEach
    void clearAutostartProperty() {
        System.clearProperty("pipeline.object-ingest.autostart");
        System.clearProperty("sun.java.command");
    }

    @Test
    void onStartupSkipsBeforeResolvingDependenciesWhenAutostartDisabled() {
        System.setProperty("pipeline.object-ingest.autostart", "false");
        ObjectIngestBootstrap bootstrap = new ObjectIngestBootstrap();
        bootstrap.orchestratorConfig = mock(PipelineOrchestratorConfig.class);
        bootstrap.executionService = mock(PipelineExecutionService.class);
        bootstrap.telemetry = mock(Instance.class);

        bootstrap.onStartup(null);

        verifyNoInteractions(bootstrap.orchestratorConfig, bootstrap.executionService, bootstrap.telemetry);
    }

    @Test
    void onStartupSkipsWhenProcessCommandRequestsIngestOnce() {
        System.setProperty("sun.java.command", "orchestrator-svc-runner.jar --ingest-once");
        ObjectIngestBootstrap bootstrap = new ObjectIngestBootstrap();
        bootstrap.orchestratorConfig = mock(PipelineOrchestratorConfig.class);
        bootstrap.executionService = mock(PipelineExecutionService.class);
        bootstrap.telemetry = mock(Instance.class);

        bootstrap.onStartup(null);

        verifyNoInteractions(bootstrap.orchestratorConfig, bootstrap.executionService, bootstrap.telemetry);
    }
}
