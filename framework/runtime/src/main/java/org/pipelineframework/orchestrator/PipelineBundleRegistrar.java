package org.pipelineframework.orchestrator;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarFile;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.pipelineframework.config.pipeline.PipelineJson;

/**
 * Validates local generated bundle artifacts before registration.
 */
@ApplicationScoped
public class PipelineBundleRegistrar {

    @Inject
    PipelineBundleManifestLoader manifestLoader;

    @Inject
    PipelineBundleArtifactStore artifactStore;

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    public PipelineBundleRecord validate(
        String tenantId,
        String pipelineId,
        String artifactPath,
        long nowEpochMs) {
        validateRequired("tenantId", tenantId);
        validateRequired("pipelineId", pipelineId);
        validateRequired("artifactPath", artifactPath);
        Path path = Path.of(artifactPath);
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException("artifactPath must be absolute");
        }
        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new IllegalArgumentException("artifactPath must point to a readable bundle JAR");
        }

        PipelineBundleManifest manifest = loadManifest(path);
        validateManifest(pipelineId, manifest);
        PipelineBundleStoredArtifact storedArtifact = artifactStore().store(path, manifest);
        return new PipelineBundleRecord(
            tenantId.trim(),
            pipelineId.trim(),
            manifest.bundleVersionId(),
            manifest.bundleHash(),
            storedArtifact.artifactPath(),
            storedArtifact.artifactSizeBytes(),
            storedArtifact.artifactChecksum(),
            PipelineBundleStatus.REGISTERED,
            manifest,
            nowEpochMs,
            nowEpochMs,
            0L);
    }

    public Uni<PipelineBundleRecord> validateAsync(
        String tenantId,
        String pipelineId,
        String artifactPath,
        long nowEpochMs) {
        return Uni.createFrom().item(() -> validate(tenantId, pipelineId, artifactPath, nowEpochMs))
            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private PipelineBundleManifest loadManifest(Path path) {
        try (JarFile jar = new JarFile(path.toFile())) {
            var entry = jar.getJarEntry(PipelineBundleManifest.RESOURCE_PATH);
            if (entry == null) {
                throw new IllegalArgumentException(
                    "Bundle JAR is missing " + PipelineBundleManifest.RESOURCE_PATH);
            }
            try (var stream = jar.getInputStream(entry)) {
                return loader().load(stream);
            }
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw e;
        } catch (java.io.IOException e) {
            throw new IllegalArgumentException("Failed to read bundle JAR manifest: " + e.getMessage(), e);
        }
    }

    private void validateManifest(String expectedPipelineId, PipelineBundleManifest manifest) {
        if (manifest.pipelineId() == null || !expectedPipelineId.trim().equals(manifest.pipelineId().trim())) {
            throw new IllegalArgumentException("Bundle pipelineId does not match request path");
        }
        if (manifest.bundleHash() == null || manifest.bundleHash().isBlank()) {
            throw new IllegalArgumentException("Bundle manifest bundleHash is required");
        }
        String expectedBundleVersionId = "sha256:" + manifest.bundleHash();
        if (!expectedBundleVersionId.equals(manifest.bundleVersionId())) {
            throw new IllegalArgumentException("Bundle manifest bundleVersionId does not match bundleHash");
        }
        if (manifest.steps() == null || manifest.steps().isEmpty()) {
            throw new IllegalArgumentException("Bundle manifest must contain ordered steps");
        }
        for (int i = 0; i < manifest.steps().size(); i++) {
            PipelineBundleStepDescriptor step = manifest.steps().get(i);
            if (step.index() != i) {
                throw new IllegalArgumentException("Bundle manifest step indexes must be contiguous and ordered");
            }
        }
        PipelineBundleCapabilities capabilities = manifest.capabilities();
        if (capabilities == null || !capabilities.localTransitionExecution()) {
            throw new IllegalArgumentException("Bundle manifest must declare local transition execution capability");
        }
        if (capabilities.transitionWorkerProtocols().stream()
            .noneMatch(protocol -> List.of("local", "rest", "grpc", "sqs").contains(protocol))) {
            throw new IllegalArgumentException("Bundle manifest must declare at least one supported worker protocol");
        }
        String computedHash = computeBundleHash(manifest);
        if (!manifest.bundleHash().equals(computedHash)) {
            throw new IllegalArgumentException("Bundle manifest bundleHash does not match canonical content");
        }
    }

    private String computeBundleHash(PipelineBundleManifest manifest) {
        try {
            return sha256(PipelineJson.mapper().writeValueAsString(canonicalContent(manifest)));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize bundle manifest canonical content", e);
        }
    }

    private Map<String, Object> canonicalContent(PipelineBundleManifest manifest) {
        Map<String, Object> content = new LinkedHashMap<>();
        content.put("schemaVersion", manifest.schemaVersion());
        content.put("pipelineId", manifest.pipelineId());
        putIfNotNull(content, "platform", manifest.platform());
        putIfNotNull(content, "transport", manifest.transport());
        putIfNotNull(content, "module", manifest.module());
        content.put("pluginHost", manifest.pluginHost());
        putIfNotNull(content, "runtimeLayout", manifest.runtimeLayout());
        content.put("steps", manifest.steps().stream().map(this::stepContent).toList());
        content.put("capabilities", capabilitiesContent(manifest.capabilities()));
        return content;
    }

    private Map<String, Object> stepContent(PipelineBundleStepDescriptor step) {
        Map<String, Object> content = new LinkedHashMap<>();
        content.put("index", step.index());
        content.put("authoredName", step.authoredName());
        putIfNotNull(content, "kind", step.kind());
        content.put("cardinality", step.cardinality());
        content.put("inputTypeId", step.inputTypeId());
        content.put("outputTypeId", step.outputTypeId());
        putIfNotNull(content, "runtimeClass", step.runtimeClass());
        putIfNotNull(content, "clientClass", step.clientClass());
        putIfNotNull(content, "awaitTransport", step.awaitTransport());
        return content;
    }

    private Map<String, Object> capabilitiesContent(PipelineBundleCapabilities capabilities) {
        Map<String, Object> content = new LinkedHashMap<>();
        content.put("localTransitionExecution", capabilities.localTransitionExecution());
        content.put("transitionWorkerProtocols", capabilities.transitionWorkerProtocols());
        return content;
    }

    private String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest algorithm is unavailable", e);
        }
    }

    private PipelineBundleManifestLoader loader() {
        return manifestLoader == null ? new PipelineBundleManifestLoader() : manifestLoader;
    }

    private PipelineBundleArtifactStore artifactStore() {
        return artifactStore == null
            ? new LocalPipelineBundleArtifactStore(PipelineBundleRuntimeBeans.storageRoot(orchestratorConfig), loader())
            : artifactStore;
    }

    private static void validateRequired(String name, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
    }

    private static void putIfNotNull(Map<String, Object> values, String key, Object value) {
        if (value != null) {
            values.put(key, value);
        }
    }
}
