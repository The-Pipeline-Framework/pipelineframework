package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.orchestrator.release.FileBackedPipelineReleaseRegistry;
import org.pipelineframework.orchestrator.release.InMemoryPipelineReleaseRegistry;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;

class PipelineReleaseRuntimeBeansTest {

    @TempDir
    Path tempDir;

    // --- storageRoot static method ---

    @Test
    void storageRootReturnsDefaultWhenConfigIsNull() {
        Path root = PipelineReleaseRuntimeBeans.storageRoot(null);
        assertEquals(Path.of(PipelineReleaseRuntimeBeans.DEFAULT_RELEASE_STORAGE_ROOT), root);
    }

    @Test
    void storageRootReturnsConfiguredPath() {
        PipelineOrchestratorConfig config = configWithStorageRoot(tempDir.toString());

        Path root = PipelineReleaseRuntimeBeans.storageRoot(config);

        assertEquals(Path.of(tempDir.toString()), root);
    }

    @Test
    void storageRootThrowsForBlankConfiguredRoot() {
        PipelineOrchestratorConfig config = configWithStorageRoot("   ");

        assertThrows(IllegalStateException.class, () ->
            PipelineReleaseRuntimeBeans.storageRoot(config));
    }

    // --- artifact store provider selection ---

    @Test
    void artifactStoreDefaultsToLocalWhenConfigIsNull() throws Exception {
        PipelineReleaseRuntimeBeans beans = beansWithConfig(null);

        PipelineReleaseArtifactStore store = beans.pipelineReleaseArtifactStore();

        assertInstanceOf(LocalPipelineReleaseArtifactStore.class, store);
    }

    @Test
    void artifactStoreUsesLocalProviderForLocalValue() throws Exception {
        PipelineOrchestratorConfig config = configWithStorageProvider("local", tempDir.toString());
        PipelineReleaseRuntimeBeans beans = beansWithConfig(config);

        PipelineReleaseArtifactStore store = beans.pipelineReleaseArtifactStore();

        assertInstanceOf(LocalPipelineReleaseArtifactStore.class, store);
    }

    @Test
    void artifactStoreUsesLocalProviderCaseInsensitive() throws Exception {
        PipelineOrchestratorConfig config = configWithStorageProvider("LOCAL", tempDir.toString());
        PipelineReleaseRuntimeBeans beans = beansWithConfig(config);

        PipelineReleaseArtifactStore store = beans.pipelineReleaseArtifactStore();

        assertInstanceOf(LocalPipelineReleaseArtifactStore.class, store);
    }

    @Test
    void artifactStoreThrowsForUnknownProvider() throws Exception {
        PipelineOrchestratorConfig config = configWithStorageProvider("redis", tempDir.toString());
        PipelineReleaseRuntimeBeans beans = beansWithConfig(config);

        assertThrows(IllegalStateException.class, beans::pipelineReleaseArtifactStore);
    }

    // --- registry provider selection ---

    @Test
    void registryDefaultsToInMemoryWhenConfigIsNull() throws Exception {
        PipelineReleaseRuntimeBeans beans = beansWithConfig(null);

        PipelineReleaseRegistry registry = beans.pipelineReleaseRegistry();

        assertInstanceOf(InMemoryPipelineReleaseRegistry.class, registry);
    }

    @Test
    void registryUsesInMemoryForMemoryProvider() throws Exception {
        PipelineOrchestratorConfig config = configWithRegistryProvider("memory", tempDir.toString());
        PipelineReleaseRuntimeBeans beans = beansWithConfig(config);

        PipelineReleaseRegistry registry = beans.pipelineReleaseRegistry();

        assertInstanceOf(InMemoryPipelineReleaseRegistry.class, registry);
    }

    @Test
    void registryUsesFileBackedForFileProvider() throws Exception {
        PipelineOrchestratorConfig config = configWithRegistryProvider("file", tempDir.toString());
        PipelineReleaseRuntimeBeans beans = beansWithConfig(config);

        PipelineReleaseRegistry registry = beans.pipelineReleaseRegistry();

        assertInstanceOf(FileBackedPipelineReleaseRegistry.class, registry);
    }

    @Test
    void registryUsesMemoryForCaseInsensitiveMemory() throws Exception {
        PipelineOrchestratorConfig config = configWithRegistryProvider("MEMORY", tempDir.toString());
        PipelineReleaseRuntimeBeans beans = beansWithConfig(config);

        PipelineReleaseRegistry registry = beans.pipelineReleaseRegistry();

        assertInstanceOf(InMemoryPipelineReleaseRegistry.class, registry);
    }

    @Test
    void registryThrowsForUnknownProvider() throws Exception {
        PipelineOrchestratorConfig config = configWithRegistryProvider("postgres", tempDir.toString());
        PipelineReleaseRuntimeBeans beans = beansWithConfig(config);

        assertThrows(IllegalStateException.class, beans::pipelineReleaseRegistry);
    }

    // --- helpers ---

    private static PipelineReleaseRuntimeBeans beansWithConfig(PipelineOrchestratorConfig config) throws Exception {
        PipelineReleaseRuntimeBeans beans = new PipelineReleaseRuntimeBeans();
        Field field = PipelineReleaseRuntimeBeans.class.getDeclaredField("orchestratorConfig");
        field.setAccessible(true);
        field.set(beans, config);
        return beans;
    }

    private static PipelineOrchestratorConfig configWithStorageRoot(String root) {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.ReleasesConfig releases = mock(PipelineOrchestratorConfig.ReleasesConfig.class);
        PipelineOrchestratorConfig.ReleaseStorageConfig storage = mock(PipelineOrchestratorConfig.ReleaseStorageConfig.class);
        when(config.releases()).thenReturn(releases);
        when(releases.storage()).thenReturn(storage);
        when(storage.provider()).thenReturn("local");
        when(storage.root()).thenReturn(root);
        return config;
    }

    private static PipelineOrchestratorConfig configWithStorageProvider(String provider, String root) {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.ReleasesConfig releases = mock(PipelineOrchestratorConfig.ReleasesConfig.class);
        PipelineOrchestratorConfig.ReleaseStorageConfig storage = mock(PipelineOrchestratorConfig.ReleaseStorageConfig.class);
        PipelineOrchestratorConfig.ReleaseRegistryConfig registry = mock(PipelineOrchestratorConfig.ReleaseRegistryConfig.class);
        when(config.releases()).thenReturn(releases);
        when(releases.storage()).thenReturn(storage);
        when(releases.registry()).thenReturn(registry);
        when(storage.provider()).thenReturn(provider);
        when(storage.root()).thenReturn(root);
        when(registry.provider()).thenReturn("memory");
        return config;
    }

    private static PipelineOrchestratorConfig configWithRegistryProvider(String registryProvider, String root) {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.ReleasesConfig releases = mock(PipelineOrchestratorConfig.ReleasesConfig.class);
        PipelineOrchestratorConfig.ReleaseStorageConfig storage = mock(PipelineOrchestratorConfig.ReleaseStorageConfig.class);
        PipelineOrchestratorConfig.ReleaseRegistryConfig registry = mock(PipelineOrchestratorConfig.ReleaseRegistryConfig.class);
        when(config.releases()).thenReturn(releases);
        when(releases.storage()).thenReturn(storage);
        when(releases.registry()).thenReturn(registry);
        when(storage.provider()).thenReturn("local");
        when(storage.root()).thenReturn(root);
        when(registry.provider()).thenReturn(registryProvider);
        return config;
    }
}