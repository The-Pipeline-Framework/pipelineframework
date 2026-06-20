package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.objectpublish.ObjectWriteRequest;
import org.pipelineframework.objectpublish.ObjectWriteResult;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

class S3ObjectTargetProviderTest {

    @Test
    void writesObjectToConfiguredBucket() {
        S3Client client = mock(S3Client.class);
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
            .thenReturn(PutObjectResponse.builder().versionId("v1").build());
        PipelineObjectPublishConfig target = new PipelineObjectPublishConfig(
            "results",
            "object",
            "s3",
            Map.of("bucket", "payments"),
            null,
            null);
        ObjectWriteRequest request = new ObjectWriteRequest(
            target.name(),
            target,
            "out/payments.csv",
            "id,amount\n1,10\n".getBytes(StandardCharsets.UTF_8),
            "text/csv",
            Map.of("groupKey", "payments.csv"),
            "checksum",
            "idempotency");

        ObjectWriteResult result = new S3ObjectTargetProvider(client).write(request).await().indefinitely();

        ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(client).putObject(captor.capture(), any(RequestBody.class));
        PutObjectRequest put = captor.getValue();
        assertEquals("payments", put.bucket());
        assertEquals("out/payments.csv", put.key());
        assertEquals("text/csv", put.contentType());
        assertEquals("payments.csv", put.metadata().get("groupKey"));
        assertEquals("results", put.metadata().get("target"));
        assertEquals("s3", result.reference().provider());
        assertEquals("payments", result.reference().container());
        assertEquals("out/payments.csv", result.reference().key());
        assertEquals("v1", result.reference().version());
    }
}
