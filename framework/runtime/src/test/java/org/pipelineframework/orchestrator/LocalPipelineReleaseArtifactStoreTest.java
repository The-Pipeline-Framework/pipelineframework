package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;
import org.pipelineframework.orchestrator.release.PipelineReleaseStatus;

class LocalPipelineReleaseArtifactStoreTest {

    @TempDir
    Path tempDir;

    // --- artifactPathFromUri ---

    @Test
    void artifactPathFromUriRejectsNull() {
        assertThrows(IllegalArgumentException.class, () ->
            LocalPipelineReleaseArtifactStore.artifactPathFromUri(null));
    }

    @Test
    void artifactPathFromUriRejectsBlank() {
        assertThrows(IllegalArgumentException.class, () ->
            LocalPipelineReleaseArtifactStore.artifactPathFromUri("   "));
    }

    @Test
    void artifactPathFromUriParsesFileSchemeUri() throws Exception {
        Path file = Files.createTempFile(tempDir, "artifact", ".jar");
        String fileUri = file.toUri().toString();
        assertTrue(fileUri.startsWith("file:"));

        Path result = LocalPipelineReleaseArtifactStore.artifactPathFromUri(fileUri);

        assertEquals(file.toAbsolutePath(), result.toAbsolutePath());
    }

    @Test
    void artifactPathFromUriParsesAbsolutePath() {
        Path result = LocalPipelineReleaseArtifactStore.artifactPathFromUri("/var/lib/tpf/releases/app.jar");
        assertEquals(Path.of("/var/lib/tpf/releases/app.jar"), result);
    }

    @Test
    void artifactPathFromUriPreservesPathSegments() {
        Path result = LocalPipelineReleaseArtifactStore.artifactPathFromUri("/opt/tpf/artifacts/sha256/abc123.jar");
        assertEquals(Path.of("/opt/tpf/artifacts/sha256/abc123.jar"), result);
    }

    // --- store ---

    @Test
    void storeRejectsNullPath() {
        LocalPipelineReleaseArtifactStore store = new LocalPipelineReleaseArtifactStore(tempDir);
        assertThrows(IllegalArgumentException.class, () -> store.store(null));
    }

    @Test
    void storeRejectsNonExistentFile() {
        LocalPipelineReleaseArtifactStore store = new LocalPipelineReleaseArtifactStore(tempDir);
        assertThrows(IllegalArgumentException.class, () ->
            store.store(tempDir.resolve("nonexistent.jar")));
    }

    @Test
    void storeReturnsFileUriForStoredArtifact() throws Exception {
        LocalPipelineReleaseArtifactStore store = new LocalPipelineReleaseArtifactStore(tempDir);
        Path artifact = Files.writeString(tempDir.resolve("app.jar"), "artifact-bytes");

        PipelineReleaseStoredArtifact stored = store.store(artifact);

        assertTrue(stored.artifactUri().startsWith("file:"),
            "stored URI should use file: scheme, got: " + stored.artifactUri());
    }

    @Test
    void storeComputesCorrectChecksumAndSize() throws Exception {
        LocalPipelineReleaseArtifactStore store = new LocalPipelineReleaseArtifactStore(tempDir);
        byte[] content = "artifact-content-for-checksum".getBytes();
        Path artifact = Files.write(tempDir.resolve("app.jar"), content);

        PipelineReleaseStoredArtifact stored = store.store(artifact);

        assertEquals("sha256:" + sha256(content), stored.artifactChecksum());
        assertEquals(content.length, stored.artifactSizeBytes());
    }

    @Test
    void storeUsesContentAddressedPath() throws Exception {
        LocalPipelineReleaseArtifactStore store = new LocalPipelineReleaseArtifactStore(tempDir);
        byte[] content = "content-addressed-artifact".getBytes();
        Path artifact = Files.write(tempDir.resolve("worker.jar"), content);
        String expectedChecksum = sha256(content);

        PipelineReleaseStoredArtifact stored = store.store(artifact);

        Path storedPath = Path.of(URI.create(stored.artifactUri()));
        assertTrue(storedPath.getFileName().toString().startsWith(expectedChecksum),
            "stored filename should start with the sha256 hex, got: " + storedPath.getFileName());
    }

    @Test
    void storeIsIdempotentForSameArtifact() throws Exception {
        LocalPipelineReleaseArtifactStore store = new LocalPipelineReleaseArtifactStore(tempDir);
        Path artifact = Files.writeString(tempDir.resolve("app.jar"), "idempotent-content");

        PipelineReleaseStoredArtifact first = store.store(artifact);
        PipelineReleaseStoredArtifact second = store.store(artifact);

        assertEquals(first.artifactUri(), second.artifactUri());
        assertEquals(first.artifactChecksum(), second.artifactChecksum());
        assertEquals(first.artifactSizeBytes(), second.artifactSizeBytes());
    }

    @Test
    void storePreservesFileExtension() throws Exception {
        LocalPipelineReleaseArtifactStore store = new LocalPipelineReleaseArtifactStore(tempDir);
        Path artifact = Files.writeString(tempDir.resolve("payment-worker.jar"), "jar-bytes");

        PipelineReleaseStoredArtifact stored = store.store(artifact);

        Path storedPath = Path.of(URI.create(stored.artifactUri()));
        assertTrue(storedPath.getFileName().toString().endsWith(".jar"),
            "stored filename should preserve .jar extension");
    }

    @Test
    void storeUsesArtifactExtensionForUnknownType() throws Exception {
        LocalPipelineReleaseArtifactStore store = new LocalPipelineReleaseArtifactStore(tempDir);
        Path artifact = Files.writeString(tempDir.resolve("worker-binary"), "binary-content");

        PipelineReleaseStoredArtifact stored = store.store(artifact);

        Path storedPath = Path.of(URI.create(stored.artifactUri()));
        assertTrue(storedPath.getFileName().toString().endsWith(".artifact"),
            "stored filename should use .artifact extension when no extension present");
    }

    // --- verify ---

    @Test
    void verifyRejectsNullRecord() {
        LocalPipelineReleaseArtifactStore store = new LocalPipelineReleaseArtifactStore(tempDir);
        assertThrows(IllegalArgumentException.class, () -> store.verify(null));
    }

    @Test
    void verifyPassesForStoredArtifact() throws Exception {
        LocalPipelineReleaseArtifactStore store = new LocalPipelineReleaseArtifactStore(tempDir);
        byte[] content = "verifiable-content".getBytes();
        Path artifact = Files.write(tempDir.resolve("verify.jar"), content);
        PipelineReleaseStoredArtifact stored = store.store(artifact);
        PipelineReleaseRecord record = releaseRecord(stored.artifactUri(), stored.artifactSizeBytes(), stored.artifactChecksum());

        store.verify(record);
    }

    @Test
    void verifyFailsForTamperedArtifact() throws Exception {
        LocalPipelineReleaseArtifactStore store = new LocalPipelineReleaseArtifactStore(tempDir);
        Path artifact = Files.writeString(tempDir.resolve("tampered.jar"), "original-content");
        PipelineReleaseStoredArtifact stored = store.store(artifact);
        Path storedPath = Path.of(URI.create(stored.artifactUri()));
        Files.writeString(storedPath, "tampered-content");
        PipelineReleaseRecord record = releaseRecord(stored.artifactUri(), stored.artifactSizeBytes(), stored.artifactChecksum());

        assertThrows(IllegalStateException.class, () -> store.verify(record));
    }

    @Test
    void verifyFailsForMissingStoredArtifact() throws Exception {
        LocalPipelineReleaseArtifactStore store = new LocalPipelineReleaseArtifactStore(tempDir);
        Path nonexistent = tempDir.resolve("artifacts/sha256/abc123.jar");
        PipelineReleaseRecord record = releaseRecord(nonexistent.toUri().toString(), 100L, "sha256:abc123");

        assertThrows(IllegalStateException.class, () -> store.verify(record));
    }

    @Test
    void verifyRejectsBlankArtifactUri() {
        LocalPipelineReleaseArtifactStore store = new LocalPipelineReleaseArtifactStore(tempDir);
        PipelineReleaseRecord record = releaseRecord("", 0L, "sha256:abc");

        assertThrows(IllegalArgumentException.class, () -> store.verify(record));
    }

    // --- constructor ---

    @Test
    void constructorRejectsNullRoot() {
        assertThrows(IllegalArgumentException.class, () ->
            new LocalPipelineReleaseArtifactStore(null));
    }

    @Test
    void constructorNormalizesRoot() {
        Path relative = Path.of("target/tpf-releases");
        LocalPipelineReleaseArtifactStore store = new LocalPipelineReleaseArtifactStore(relative);
        assertTrue(store.root().isAbsolute(), "root should be an absolute path after normalization");
    }

    // --- helpers ---

    private static PipelineReleaseRecord releaseRecord(String uri, long size, String checksum) {
        PipelineContractDescriptor contract = PipelineContractDescriptor.localFallback();
        PipelineReleaseArtifactDescriptor artifact = new PipelineReleaseArtifactDescriptor(
            "app", "jar", uri, checksum, List.of(), List.of());
        PipelineReleaseDescriptor descriptor = new PipelineReleaseDescriptor(
            PipelineReleaseDescriptor.CURRENT_SCHEMA_VERSION,
            contract.pipelineId(),
            contract.contractVersion(),
            "release-1",
            List.of(artifact));
        return new PipelineReleaseRecord(
            "tenant-1",
            contract.pipelineId(),
            contract.contractVersion(),
            "release-1",
            PipelineReleaseStatus.REGISTERED,
            descriptor,
            artifact.artifactId(),
            artifact.digest(),
            uri,
            size,
            checksum,
            contract,
            1000L,
            1000L,
            0L);
    }

    private static String sha256(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(bytes));
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}