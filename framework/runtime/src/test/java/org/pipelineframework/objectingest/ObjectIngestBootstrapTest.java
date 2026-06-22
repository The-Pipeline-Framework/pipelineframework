package org.pipelineframework.objectingest;

import jakarta.enterprise.inject.Instance;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class ObjectIngestBootstrapTest {
    private String originalAutostart;
    private String originalJavaCommand;

    @BeforeEach
    void captureSystemProperties() {
        originalAutostart = System.getProperty("pipeline.object-ingest.autostart");
        originalJavaCommand = System.getProperty("sun.java.command");
    }

    @AfterEach
    void clearAutostartProperty() {
        restoreProperty("pipeline.object-ingest.autostart", originalAutostart);
        restoreProperty("sun.java.command", originalJavaCommand);
    }

    private static void restoreProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
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
