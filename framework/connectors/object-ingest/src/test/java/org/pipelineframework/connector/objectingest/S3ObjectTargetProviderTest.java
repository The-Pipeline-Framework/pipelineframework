package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Map;

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

        session.write(ByteBuffer.wrap(new byte[5 * 1024 * 1024])).toCompletableFuture().join();
        ObjectWriteResult result = session.close(new ObjectWriteCloseRequest(
            5 * 1024 * 1024,
            "checksum",
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

    private ObjectWriteOpenRequest openRequest() {
        PipelineObjectPublishConfig target = new PipelineObjectPublishConfig(
            "results",
            "object",
            "s3",
            Map.of("bucket", "payments", "prefix", "out"),
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
}
