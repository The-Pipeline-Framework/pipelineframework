package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.worker.DynamoPipelineWorkerRegistry;
import org.pipelineframework.orchestrator.worker.InMemoryPipelineWorkerRegistry;
import org.pipelineframework.orchestrator.worker.PipelineWorkerRegistry;

class PipelineReleaseRuntimeBeansWorkerTest {

    // --- workerStaleAfter static helper tests ---

    @Test
    void workerStaleAfterReturnsDefaultWhenConfigIsNull() {
        Duration result = PipelineReleaseRuntimeBeans.workerStaleAfter(null);
        assertEquals(Duration.ofMinutes(2), result);
    }

    @Test
    void workerStaleAfterReturnsDefaultWhenWorkerConfigIsNull() {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        when(config.worker()).thenReturn(null);

        Duration result = PipelineReleaseRuntimeBeans.workerStaleAfter(config);
        assertEquals(Duration.ofMinutes(2), result);
    }

    @Test
    void workerStaleAfterReturnsDefaultWhenLifecycleConfigIsNull() {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.WorkerConfig workerConfig = mock(PipelineOrchestratorConfig.WorkerConfig.class);
        when(config.worker()).thenReturn(workerConfig);
        when(workerConfig.lifecycle()).thenReturn(null);

        Duration result = PipelineReleaseRuntimeBeans.workerStaleAfter(config);
        assertEquals(Duration.ofMinutes(2), result);
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

        Duration result = PipelineReleaseRuntimeBeans.workerStaleAfter(config);
        assertEquals(Duration.ofMinutes(2), result);
    }

    @Test
    void workerStaleAfterReturnsConfiguredValue() {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.WorkerConfig workerConfig = mock(PipelineOrchestratorConfig.WorkerConfig.class);
        PipelineOrchestratorConfig.WorkerLifecycleConfig lifecycleConfig =
            mock(PipelineOrchestratorConfig.WorkerLifecycleConfig.class);
        when(config.worker()).thenReturn(workerConfig);
        when(workerConfig.lifecycle()).thenReturn(lifecycleConfig);
        when(lifecycleConfig.staleAfter()).thenReturn(Duration.ofSeconds(30));

        Duration result = PipelineReleaseRuntimeBeans.workerStaleAfter(config);
        assertEquals(Duration.ofSeconds(30), result);
    }

    // --- pipelineWorkerRegistry factory method tests ---

    @Test
    void workerRegistryDefaultsToInMemoryWhenConfigIsNull() {
        PipelineReleaseRuntimeBeans beans = new PipelineReleaseRuntimeBeans();
        beans.orchestratorConfig = null;

        PipelineWorkerRegistry registry = beans.pipelineWorkerRegistry();
        assertInstanceOf(InMemoryPipelineWorkerRegistry.class, registry);
    }

    @Test
    void workerRegistryDefaultsToInMemoryWhenWorkerConfigIsNull() {
        PipelineReleaseRuntimeBeans beans = new PipelineReleaseRuntimeBeans();
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        when(config.worker()).thenReturn(null);
        beans.orchestratorConfig = config;

        PipelineWorkerRegistry registry = beans.pipelineWorkerRegistry();
        assertInstanceOf(InMemoryPipelineWorkerRegistry.class, registry);
    }

    @Test
    void workerRegistryDefaultsToInMemoryWhenLifecycleConfigIsNull() {
        PipelineReleaseRuntimeBeans beans = new PipelineReleaseRuntimeBeans();
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.WorkerConfig workerConfig = mock(PipelineOrchestratorConfig.WorkerConfig.class);
        when(config.worker()).thenReturn(workerConfig);
        when(workerConfig.lifecycle()).thenReturn(null);
        beans.orchestratorConfig = config;

        PipelineWorkerRegistry registry = beans.pipelineWorkerRegistry();
        assertInstanceOf(InMemoryPipelineWorkerRegistry.class, registry);
    }

    @Test
    void workerRegistryCreatesInMemoryWhenProviderIsMemory() {
        PipelineReleaseRuntimeBeans beans = beansWithProvider("memory");

        PipelineWorkerRegistry registry = beans.pipelineWorkerRegistry();
        assertInstanceOf(InMemoryPipelineWorkerRegistry.class, registry);
    }

    @Test
    void workerRegistryCreatesInMemoryWhenProviderIsMemoryCaseInsensitive() {
        PipelineReleaseRuntimeBeans beans = beansWithProvider("MEMORY");

        PipelineWorkerRegistry registry = beans.pipelineWorkerRegistry();
        assertInstanceOf(InMemoryPipelineWorkerRegistry.class, registry);
    }

    @Test
    void workerRegistryCreatesDynamoWhenProviderIsDynamo() {
        PipelineReleaseRuntimeBeans beans = beansWithDynamoProvider();

        PipelineWorkerRegistry registry = beans.pipelineWorkerRegistry();
        assertInstanceOf(DynamoPipelineWorkerRegistry.class, registry);
    }

    @Test
    void workerRegistryThrowsWhenProviderIsUnknown() {
        PipelineReleaseRuntimeBeans beans = beansWithProvider("redis");

        assertThrows(IllegalStateException.class, beans::pipelineWorkerRegistry);
    }

    private static PipelineReleaseRuntimeBeans beansWithProvider(String provider) {
        PipelineReleaseRuntimeBeans beans = new PipelineReleaseRuntimeBeans();
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.WorkerConfig workerConfig = mock(PipelineOrchestratorConfig.WorkerConfig.class);
        PipelineOrchestratorConfig.WorkerLifecycleConfig lifecycleConfig =
            mock(PipelineOrchestratorConfig.WorkerLifecycleConfig.class);
        when(config.worker()).thenReturn(workerConfig);
        when(workerConfig.lifecycle()).thenReturn(lifecycleConfig);
        when(lifecycleConfig.provider()).thenReturn(provider);
        beans.orchestratorConfig = config;
        return beans;
    }

    private static PipelineReleaseRuntimeBeans beansWithDynamoProvider() {
        PipelineReleaseRuntimeBeans beans = new PipelineReleaseRuntimeBeans();
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.WorkerConfig workerConfig = mock(PipelineOrchestratorConfig.WorkerConfig.class);
        PipelineOrchestratorConfig.WorkerLifecycleConfig lifecycleConfig =
            mock(PipelineOrchestratorConfig.WorkerLifecycleConfig.class);
        PipelineOrchestratorConfig.DynamoConfig dynamoConfig = mock(PipelineOrchestratorConfig.DynamoConfig.class);
        when(config.worker()).thenReturn(workerConfig);
        when(workerConfig.lifecycle()).thenReturn(lifecycleConfig);
        when(lifecycleConfig.provider()).thenReturn("dynamo");
        when(config.dynamo()).thenReturn(dynamoConfig);
        when(dynamoConfig.workerTable()).thenReturn("tpf_worker_registry");
        beans.orchestratorConfig = config;
        return beans;
    }
}