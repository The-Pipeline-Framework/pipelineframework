package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.pipeline.PipelineJson;

class PipelineBundleArtifactStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void copiesValidJarAndVerifiesStoredIntegrity() throws Exception {
        PipelineBundleManifest manifest = manifest();
        Path source = bundleJar(manifest, tempDir.resolve("source.jar"));
        LocalPipelineBundleArtifactStore store = new LocalPipelineBundleArtifactStore(
            tempDir.resolve("store"),
            new PipelineBundleManifestLoader());

        PipelineBundleStoredArtifact stored = store.store(source, manifest);

        assertNotEquals(source.toString(), stored.artifactPath());
        assertTrue(Files.isRegularFile(Path.of(stored.artifactPath())));
        assertEquals(Files.size(Path.of(stored.artifactPath())), stored.artifactSizeBytes());

        PipelineBundleRecord record = record(manifest, stored);
        store.verify(record);

        Files.delete(source);
        store.verify(record);
    }

    @Test
    void verifyFailsWhenStoredArtifactIsMissingOrTampered() throws Exception {
        PipelineBundleManifest manifest = manifest();
        Path source = bundleJar(manifest, tempDir.resolve("source.jar"));
        LocalPipelineBundleArtifactStore store = new LocalPipelineBundleArtifactStore(
            tempDir.resolve("store"),
            new PipelineBundleManifestLoader());
        PipelineBundleStoredArtifact stored = store.store(source, manifest);
        PipelineBundleRecord record = record(manifest, stored);

        Files.writeString(Path.of(stored.artifactPath()), "tampered", StandardCharsets.UTF_8);
        assertThrows(IllegalStateException.class, () -> store.verify(record));

        Files.delete(Path.of(stored.artifactPath()));
        assertThrows(IllegalStateException.class, () -> store.verify(record));
    }

    private static PipelineBundleRecord record(PipelineBundleManifest manifest, PipelineBundleStoredArtifact stored) {
        return new PipelineBundleRecord(
            "tenant-1",
            manifest.pipelineId(),
            manifest.bundleVersionId(),
            manifest.bundleHash(),
            stored.artifactPath(),
            stored.artifactSizeBytes(),
            stored.artifactChecksum(),
            PipelineBundleStatus.REGISTERED,
            manifest,
            1L,
            1L,
            0L);
    }

    private static PipelineBundleManifest manifest() throws Exception {
        Map<String, Object> withoutHash = canonicalContent();
        String hash = sha256(PipelineJson.mapper().writeValueAsString(withoutHash));
        return new PipelineBundleManifest(
            PipelineBundleManifest.CURRENT_SCHEMA_VERSION,
            "org.example.pipeline",
            "sha256:" + hash,
            hash,
            "COMPUTE",
            "REST",
            "module",
            false,
            "monolith",
            List.of(new PipelineBundleStepDescriptor(
                0,
                "Validate",
                "service",
                "ONE_TO_ONE",
                "Input",
                "Output",
                "Runtime",
                "Client",
                null)),
            PipelineBundleCapabilities.defaults());
    }

    private static Path bundleJar(PipelineBundleManifest manifest, Path jar) throws Exception {
        Map<String, Object> finalManifest = canonicalContent();
        finalManifest.put("bundleVersionId", manifest.bundleVersionId());
        finalManifest.put("bundleHash", manifest.bundleHash());
        try (OutputStream output = Files.newOutputStream(jar);
            JarOutputStream jarOutput = new JarOutputStream(output)) {
            jarOutput.putNextEntry(new JarEntry(PipelineBundleManifest.RESOURCE_PATH));
            jarOutput.write(PipelineJson.mapper().writeValueAsString(finalManifest).getBytes(StandardCharsets.UTF_8));
            jarOutput.closeEntry();
        }
        return jar;
    }

    private static Map<String, Object> canonicalContent() {
        Map<String, Object> content = new LinkedHashMap<>();
        content.put("schemaVersion", 1);
        content.put("pipelineId", "org.example.pipeline");
        content.put("platform", "COMPUTE");
        content.put("transport", "REST");
        content.put("module", "module");
        content.put("pluginHost", false);
        content.put("runtimeLayout", "monolith");
        content.put("steps", List.of(step()));
        content.put("capabilities", capabilities());
        return content;
    }

    private static Map<String, Object> step() {
        Map<String, Object> step = new LinkedHashMap<>();
        step.put("index", 0);
        step.put("authoredName", "Validate");
        step.put("kind", "service");
        step.put("cardinality", "ONE_TO_ONE");
        step.put("inputTypeId", "Input");
        step.put("outputTypeId", "Output");
        step.put("runtimeClass", "Runtime");
        step.put("clientClass", "Client");
        return step;
    }

    private static Map<String, Object> capabilities() {
        Map<String, Object> capabilities = new LinkedHashMap<>();
        capabilities.put("localTransitionExecution", true);
        capabilities.put("transitionWorkerProtocols", List.of("local", "rest", "grpc", "sqs"));
        return capabilities;
    }

    private static String sha256(String value) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
    }
}
