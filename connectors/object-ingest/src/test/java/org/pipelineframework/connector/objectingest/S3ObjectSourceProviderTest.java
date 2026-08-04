package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.boundary.PipelineObjectFilterConfig;
import org.pipelineframework.config.boundary.PipelineObjectSourceConfig;
import org.pipelineframework.objectingest.ObjectSourceItem;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

class S3ObjectSourceProviderTest {

    @Test
    void mapsListedS3ObjectsToSourceItems() {
        S3Client client = mock(S3Client.class);
        when(client.listObjectsV2(any(software.amazon.awssdk.services.s3.model.ListObjectsV2Request.class)))
            .thenReturn(ListObjectsV2Response.builder()
                .contents(S3Object.builder()
                    .key("raw/doc.txt")
                    .eTag("\"abc123\"")
                    .size(42L)
                    .lastModified(Instant.ofEpochMilli(1234L))
                    .build())
                .build());
        PipelineObjectSourceConfig source = new PipelineObjectSourceConfig(
            "search-documents",
            "object",
            "s3",
            Map.of("bucket", "docs", "prefix", "raw/"),
            new PipelineObjectFilterConfig(List.of("**/*.txt"), List.of()),
            null,
            null,
            null);

        List<ObjectSourceItem> items = new S3ObjectSourceProvider(client).list(source, 10);

        assertEquals(1, items.size());
        ObjectSourceItem item = items.getFirst();
        assertEquals("s3", item.provider());
        assertEquals("docs", item.container());
        assertEquals("raw/doc.txt", item.key());
        assertEquals("abc123", item.etag());
        assertEquals(42L, item.sizeBytes());
        assertEquals("docs", item.contentRef().container());
    }

    @Test
    void paginatesUntilLimitMatchingObjects() {
        S3Client client = mock(S3Client.class);
        when(client.listObjectsV2(any(software.amazon.awssdk.services.s3.model.ListObjectsV2Request.class)))
            .thenReturn(
                ListObjectsV2Response.builder()
                    .contents(S3Object.builder().key("raw/ignored.json").size(10L).build())
                    .nextContinuationToken("page-2")
                    .build(),
                ListObjectsV2Response.builder()
                    .contents(S3Object.builder()
                        .key("raw/doc.txt")
                        .eTag("\"abc123\"")
                        .size(42L)
                        .lastModified(Instant.ofEpochMilli(1234L))
                        .build())
                    .build());
        PipelineObjectSourceConfig source = new PipelineObjectSourceConfig(
            "search-documents",
            "object",
            "s3",
            Map.of("bucket", "docs", "prefix", "raw/"),
            new PipelineObjectFilterConfig(List.of("**/*.txt"), List.of()),
            null,
            null,
            null);

        List<ObjectSourceItem> items = new S3ObjectSourceProvider(client).list(source, 1);

        assertEquals(1, items.size());
        assertEquals("raw/doc.txt", items.getFirst().key());
    }

    @Test
    void rejectsOversizedReadsBeforeDownloadingObject() {
        S3Client client = mock(S3Client.class);
        when(client.headObject(any(software.amazon.awssdk.services.s3.model.HeadObjectRequest.class)))
            .thenReturn(HeadObjectResponse.builder().contentLength(100L).build());
        PipelineObjectSourceConfig source = new PipelineObjectSourceConfig(
            "search-documents",
            "object",
            "s3",
            Map.of("bucket", "docs"),
            null,
            null,
            null,
            null);

        assertThrows(IllegalStateException.class,
            () -> new S3ObjectSourceProvider(client).readText(source, item("raw/doc.txt"), 10L));
        verify(client, never()).getObjectAsBytes(any(GetObjectRequest.class));
    }

    private ObjectSourceItem item(String key) {
        return new ObjectSourceItem(
            "s3",
            "docs",
            key,
            null,
            "etag",
            1L,
            1L,
            "text/plain",
            Map.of(),
            null,
            null);
    }
}
