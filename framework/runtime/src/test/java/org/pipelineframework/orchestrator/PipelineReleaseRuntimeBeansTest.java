package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.worker.DynamoPipelineWorkerRegistry;
import org.pipelineframework.orchestrator.worker.InMemoryPipelineWorkerRegistry;

class PipelineReleaseRuntimeBeansTest {

    @Test
    void workerStaleAfterReturnsDefaultWhenConfigIsNull() {
        assertEquals(Duration.ofMinutes(2), PipelineReleaseRuntimeBeans.workerStaleAfter(null));
    }

    @Test
    void workerStaleAfterReturnsDefaultWhenWorkerConfigIsNull() {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        when(config.worker()).thenReturn(null);

        assertEquals(Duration.ofMinutes(2), PipelineReleaseRuntimeBeans.workerStaleAfter(config));
    }

    @Test
    void workerStaleAfterReturnsDefaultWhenLifecycleConfigIsNull() {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.WorkerConfig workerConfig = mock(PipelineOrchestratorConfig.WorkerConfig.class);
        when(config.worker()).thenReturn(workerConfig);
        when(workerConfig.lifecycle()).thenReturn(null);

        assertEquals(Duration.ofMinutes(2), PipelineReleaseRuntimeBeans.workerStaleAfter(config));
    }

    @Test
    void workerStaleAfterReturnsDefaultWhenStaleAfterIsNull() {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.WorkerConfig workerConfig = mock(PipelineOrchestratorConfig.WorkerConfig.class);
        PipelineOrchestratorConfig.WorkerLifecycleConfig lifecycleConfig =
            mock(PipelineOrchestratorConfig.WorkerLifecycleConfig.class);
        when(config.worker()).thenReturn(workerConfig);
        when(workerConfig.lifecycle()).thenReturn(lifecycleConfig);
        when(lifecycleConfig.staleAfter()).thenReturn(null);

        assertEquals(Duration.ofMinutes(2), PipelineReleaseRuntimeBeans.workerStaleAfter(config));
    }

    @Test
    void workerStaleAfterReturnsConfiguredDuration() {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.WorkerConfig workerConfig = mock(PipelineOrchestratorConfig.WorkerConfig.class);
        PipelineOrchestratorConfig.WorkerLifecycleConfig lifecycleConfig =
            mock(PipelineOrchestratorConfig.WorkerLifecycleConfig.class);
        when(config.worker()).thenReturn(workerConfig);
        when(workerConfig.lifecycle()).thenReturn(lifecycleConfig);
        when(lifecycleConfig.staleAfter()).thenReturn(Duration.ofSeconds(30));

        assertEquals(Duration.ofSeconds(30), PipelineReleaseRuntimeBeans.workerStaleAfter(config));
    }

    @Test
    void workerRegistryProducerSelectsMemoryProviderByDefault() {
        PipelineReleaseRuntimeBeans beans = new PipelineReleaseRuntimeBeans();
        beans.orchestratorConfig = null;

        assertEquals(InMemoryPipelineWorkerRegistry.class, beans.pipelineWorkerRegistry().getClass());
    }

    @Test
    void workerRegistryProducerSelectsMemoryProviderWhenExplicitlyConfigured() {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.WorkerConfig workerConfig = mock(PipelineOrchestratorConfig.WorkerConfig.class);
        PipelineOrchestratorConfig.WorkerLifecycleConfig lifecycleConfig =
            mock(PipelineOrchestratorConfig.WorkerLifecycleConfig.class);
        when(config.worker()).thenReturn(workerConfig);
        when(workerConfig.lifecycle()).thenReturn(lifecycleConfig);
        when(lifecycleConfig.provider()).thenReturn("memory");

        PipelineReleaseRuntimeBeans beans = new PipelineReleaseRuntimeBeans();
        beans.orchestratorConfig = config;

        assertEquals(InMemoryPipelineWorkerRegistry.class, beans.pipelineWorkerRegistry().getClass());
    }

    @Test
    void workerRegistryProducerSelectsDynamoProviderWhenConfigured() {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.WorkerConfig workerConfig = mock(PipelineOrchestratorConfig.WorkerConfig.class);
        PipelineOrchestratorConfig.WorkerLifecycleConfig lifecycleConfig =
            mock(PipelineOrchestratorConfig.WorkerLifecycleConfig.class);
        when(config.worker()).thenReturn(workerConfig);
        when(workerConfig.lifecycle()).thenReturn(lifecycleConfig);
        when(lifecycleConfig.provider()).thenReturn("dynamo");

        PipelineReleaseRuntimeBeans beans = new PipelineReleaseRuntimeBeans();
        beans.orchestratorConfig = config;

        assertEquals(DynamoPipelineWorkerRegistry.class, beans.pipelineWorkerRegistry().getClass());
    }

    @Test
    void workerRegistryProducerThrowsForUnknownProvider() {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.WorkerConfig workerConfig = mock(PipelineOrchestratorConfig.WorkerConfig.class);
        PipelineOrchestratorConfig.WorkerLifecycleConfig lifecycleConfig =
            mock(PipelineOrchestratorConfig.WorkerLifecycleConfig.class);
        when(config.worker()).thenReturn(workerConfig);
        when(workerConfig.lifecycle()).thenReturn(lifecycleConfig);
        when(lifecycleConfig.provider()).thenReturn("unknown-provider");

        PipelineReleaseRuntimeBeans beans = new PipelineReleaseRuntimeBeans();
        beans.orchestratorConfig = config;

        IllegalStateException e = assertThrows(IllegalStateException.class, beans::pipelineWorkerRegistry);
        assertTrue(e.getMessage().contains("unknown-provider"));
    }
}