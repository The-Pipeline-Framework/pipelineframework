package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PipelineBundleIdentityResolverTest {

    @Test
    void usesManifestIdentityWhenConfigHasDefaults() {
        PipelineBundleIdentityResolver resolver = resolverWithManifest(manifest());
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        when(config.pipelineId()).thenReturn(PipelineBundleManifest.DEFAULT_PIPELINE_ID);
        when(config.bundleVersionId()).thenReturn(PipelineBundleManifest.DEFAULT_BUNDLE_VERSION_ID);

        assertEquals("org.example.restaurant", resolver.pipelineId(config));
        assertEquals("sha256:manifest", resolver.bundleVersionId(config));
    }

    @Test
    void explicitConfigOverridesManifestIdentity() {
        PipelineBundleIdentityResolver resolver = resolverWithManifest(manifest());
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        when(config.pipelineId()).thenReturn("override-pipeline");
        when(config.bundleVersionId()).thenReturn("override-bundle");

        assertEquals("override-pipeline", resolver.pipelineId(config));
        assertEquals("override-bundle", resolver.bundleVersionId(config));
    }

    @Test
    void detectsCommandIdentityMismatch() {
        PipelineBundleIdentityResolver resolver = resolverWithManifest(manifest());
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        when(config.pipelineId()).thenReturn(PipelineBundleManifest.DEFAULT_PIPELINE_ID);
        when(config.bundleVersionId()).thenReturn(PipelineBundleManifest.DEFAULT_BUNDLE_VERSION_ID);
        TransitionCommandEnvelope command = new TransitionCommandEnvelope(
            "tenant-1",
            "exec-1",
            "other-pipeline",
            "sha256:other",
            0,
            0,
            ExecutionResultShape.SINGLE,
            0L,
            "transition-1",
            "trace-1",
            "null",
            JsonTransitionPayloadCodec.ENCODING,
            "null");

        Optional<String> mismatch = resolver.validateCommandIdentity(command, config);

        assertTrue(mismatch.isPresent());
        assertTrue(mismatch.get().contains("other-pipeline"));
        assertTrue(mismatch.get().contains("org.example.restaurant"));
    }

    private static PipelineBundleIdentityResolver resolverWithManifest(PipelineBundleManifest manifest) {
        PipelineBundleManifestLoader loader = mock(PipelineBundleManifestLoader.class);
        when(loader.load()).thenReturn(Optional.of(manifest));
        PipelineBundleIdentityResolver resolver = new PipelineBundleIdentityResolver();
        resolver.manifestLoader = loader;
        return resolver;
    }

    private static PipelineBundleManifest manifest() {
        return new PipelineBundleManifest(
            1,
            "org.example.restaurant",
            "sha256:manifest",
            "manifest",
            "COMPUTE",
            "REST",
            "monolith-svc",
            false,
            "MONOLITH",
            List.of(),
            PipelineBundleCapabilities.defaults());
    }
}
