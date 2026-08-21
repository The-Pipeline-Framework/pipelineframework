package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.objectpublish.ObjectWriteCloseRequest;
import org.pipelineframework.objectpublish.ObjectWriteOpenRequest;
import org.pipelineframework.objectpublish.ObjectWriteResult;
import org.pipelineframework.objectpublish.ObjectWriteSession;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

class S3ObjectTargetProviderTest {

    @Test
    void streamsMultipartUploadAndCompletesOnClose() {
        S3Client client = mock(S3Client.class);
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload-1").build());
        when(client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
            .thenReturn(UploadPartResponse.builder().eTag("etag-1").build());
        S3ObjectTargetProvider provider = new S3ObjectTargetProvider(client, Runnable::run, 5 * 1024 * 1024);
        ObjectWriteSession session = provider.open(openRequest()).toCompletableFuture().join();

        byte[] payload = new byte[5 * 1024 * 1024];
        session.write(ByteBuffer.wrap(payload)).toCompletableFuture().join();
        ObjectWriteResult result = session.close(new ObjectWriteCloseRequest(
            payload.length,
            sha256(payload),
            Map.of("recordCount", "1"))).toCompletableFuture().join();

        ArgumentCaptor<CreateMultipartUploadRequest> createCaptor =
            ArgumentCaptor.forClass(CreateMultipartUploadRequest.class);
        verify(client).createMultipartUpload(createCaptor.capture());
        assertEquals("payments", createCaptor.getValue().bucket());
        assertEquals("out/payments.csv", createCaptor.getValue().key());
        verify(client).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        verify(client).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        assertEquals("s3", result.reference().provider());
        assertEquals("payments", result.reference().container());
        assertEquals("out/payments.csv", result.reference().key());
        assertEquals("1", result.reference().metadata().get("recordCount"));
        assertEquals(
            S3ObjectSourceProvider.CHECKSUM_KIND_SHA256,
            result.reference().metadata().get(S3ObjectSourceProvider.CHECKSUM_KIND_METADATA));
        assertEquals(sha256(payload), result.reference().checksum());
    }

    @Test
    void abortsMultipartUploadWhenCallerChecksumDoesNotMatchWrittenBytes() {
        S3Client client = mock(S3Client.class);
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload-1").build());
        when(client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
            .thenReturn(UploadPartResponse.builder().eTag("etag-1").build());
        RuntimeException abortFailure = new RuntimeException("abort failed");
        doThrow(abortFailure).when(client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
        S3ObjectTargetProvider provider = new S3ObjectTargetProvider(client, Runnable::run, 5 * 1024 * 1024);
        ObjectWriteSession session = provider.open(openRequest()).toCompletableFuture().join();
        byte[] payload = new byte[5 * 1024 * 1024];
        session.write(ByteBuffer.wrap(payload)).toCompletableFuture().join();

        CompletionException failure = assertThrows(CompletionException.class, () -> session.close(
            new ObjectWriteCloseRequest(payload.length, "not-the-digest", Map.of())).toCompletableFuture().join());

        assertEquals("S3 written payload checksum mismatch", failure.getCause().getMessage());
        assertSame(abortFailure, failure.getCause().getSuppressed()[0]);
        verify(client).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        verify(client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
        verify(client, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
    }

    @Test
    void trimsConfiguredRegionInPublishedReferenceMetadata() {
        S3Client client = mock(S3Client.class);
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload-1").build());
        S3ObjectTargetProvider provider = new S3ObjectTargetProvider(client, Runnable::run, 5 * 1024 * 1024);
        ObjectWriteSession session = provider.open(openRequest("  eu-west-1  ")).toCompletableFuture().join();

        ObjectWriteResult result = session.close(new ObjectWriteCloseRequest(0, sha256(new byte[0]), Map.of()))
            .toCompletableFuture().join();

        assertEquals("eu-west-1", result.reference().metadata().get(S3ObjectSourceProvider.REGION_METADATA));
    }

    @Test
    void abortCancelsMultipartUpload() {
        S3Client client = mock(S3Client.class);
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload-1").build());
        S3ObjectTargetProvider provider = new S3ObjectTargetProvider(client, Runnable::run, 5 * 1024 * 1024);
        ObjectWriteSession session = provider.open(openRequest()).toCompletableFuture().join();

        session.abort(new RuntimeException("failed")).toCompletableFuture().join();

        verify(client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }

    @Test
    void rejectsClientResolutionAfterClose() {
        S3ObjectTargetProvider provider = new S3ObjectTargetProvider(mock(S3Client.class), Runnable::run, 5 * 1024 * 1024);
        provider.close();

        CompletionException exception = assertThrows(CompletionException.class,
            () -> provider.open(openRequest()).toCompletableFuture().join());

        assertEquals("S3 object target provider is closed", exception.getCause().getMessage());
    }

    private ObjectWriteOpenRequest openRequest() {
        return openRequest(null);
    }

    private ObjectWriteOpenRequest openRequest(String region) {
        Map<String, Object> location = new java.util.LinkedHashMap<>();
        location.put("bucket", "payments");
        location.put("prefix", "out");
        if (region != null) {
            location.put("region", region);
        }
        PipelineObjectPublishConfig target = new PipelineObjectPublishConfig(
            "results",
            "object",
            "s3",
            Map.copyOf(location),
            null,
            null);
        return new ObjectWriteOpenRequest(
            target.name(),
            target,
            "payments.csv",
            "text/csv",
            Map.of("groupKey", "payments.csv"),
            "idempotency");
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (java.security.NoSuchAlgorithmException failure) {
            throw new IllegalStateException(failure);
        }
    }
}
