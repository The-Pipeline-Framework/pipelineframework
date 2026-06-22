package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
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

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

class S3PipelineReleaseArtifactStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void storesContentAddressedArtifactAndReturnsS3Uri() throws Exception {
        S3Client client = mock(S3Client.class);
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
            .thenReturn(PutObjectResponse.builder().build());
        Path artifact = Files.writeString(tempDir.resolve("app.jar"), "artifact-content");
        String checksum = sha256(Files.readAllBytes(artifact));
        S3PipelineReleaseArtifactStore store = new S3PipelineReleaseArtifactStore(client, "tpf-artifacts", "tenant/releases");

        PipelineReleaseStoredArtifact stored = store.store(artifact);

        assertEquals("sha256:" + checksum, stored.artifactChecksum());
        assertEquals(Files.size(artifact), stored.artifactSizeBytes());
        assertEquals("s3://tpf-artifacts/tenant/releases/sha256/" + checksum + ".jar", stored.artifactUri());
        verify(client).putObject(argThat((PutObjectRequest request) ->
            "tpf-artifacts".equals(request.bucket())
                && request.key().equals("tenant/releases/sha256/" + checksum + ".jar")
                && checksum.equals(request.metadata().get("sha256"))), any(RequestBody.class));
    }

    @Test
    void verifiesStoredObjectSizeAndChecksum() {
        S3Client client = mock(S3Client.class);
        byte[] bytes = "artifact-content".getBytes(StandardCharsets.UTF_8);
        PipelineReleaseRecord record = releaseRecord(
            "s3://tpf-artifacts/tenant/releases/sha256/" + sha256(bytes) + ".jar",
            bytes.length,
            "sha256:" + sha256(bytes));
        when(client.headObject(any(HeadObjectRequest.class)))
            .thenReturn(HeadObjectResponse.builder().contentLength((long) bytes.length).build());
        when(client.getObject(any(GetObjectRequest.class)))
            .thenReturn(response(bytes));
        S3PipelineReleaseArtifactStore store = new S3PipelineReleaseArtifactStore(client, "tpf-artifacts", "tenant/releases");

        store.verify(record);

        verify(client).headObject(argThat((HeadObjectRequest request) ->
            "tpf-artifacts".equals(request.bucket())
                && request.key().startsWith("tenant/releases/sha256/")));
    }

    @Test
    void rejectsMissingStoredObject() {
        S3Client client = mock(S3Client.class);
        PipelineReleaseRecord record = releaseRecord("s3://tpf-artifacts/releases/app.jar", 1L, "sha256:abc");
        when(client.headObject(any(HeadObjectRequest.class)))
            .thenThrow(S3Exception.builder().statusCode(404).message("not found").build());
        S3PipelineReleaseArtifactStore store = new S3PipelineReleaseArtifactStore(client, "tpf-artifacts", "releases");

        assertThrows(IllegalStateException.class, () -> store.verify(record));
    }

    @Test
    void rejectsTamperedStoredObject() {
        S3Client client = mock(S3Client.class);
        byte[] bytes = "artifact-content".getBytes(StandardCharsets.UTF_8);
        PipelineReleaseRecord record = releaseRecord(
            "s3://tpf-artifacts/releases/app.jar",
            bytes.length,
            "sha256:" + sha256("different".getBytes(StandardCharsets.UTF_8)));
        when(client.headObject(any(HeadObjectRequest.class)))
            .thenReturn(HeadObjectResponse.builder().contentLength((long) bytes.length).build());
        when(client.getObject(any(GetObjectRequest.class)))
            .thenReturn(response(bytes));
        S3PipelineReleaseArtifactStore store = new S3PipelineReleaseArtifactStore(client, "tpf-artifacts", "releases");

        assertThrows(IllegalStateException.class, () -> store.verify(record));
    }

    @Test
    void rejectsNegativeStoredArtifactSize() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineReleaseStoredArtifact("s3://bucket/key", -1L, "sha256:abc"));
    }

    private static ResponseInputStream<GetObjectResponse> response(byte[] bytes) {
        return new ResponseInputStream<>(
            GetObjectResponse.builder().contentLength((long) bytes.length).build(),
            AbortableInputStream.create(new ByteArrayInputStream(bytes)));
    }

    private static PipelineReleaseRecord releaseRecord(String uri, long size, String checksum) {
        PipelineContractDescriptor contract = PipelineContractDescriptor.localFallback();
        PipelineReleaseArtifactDescriptor artifact = new PipelineReleaseArtifactDescriptor(
            "app",
            "jar",
            uri,
            checksum,
            List.of(),
            List.of());
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
            throw new IllegalStateException("SHA-256 digest algorithm is unavailable", e);
        }
    }
}
