package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.boundary.PipelineObjectFilterConfig;
import org.pipelineframework.config.boundary.PipelineObjectSourceConfig;
import org.pipelineframework.connector.MaterializedPayload;
import org.pipelineframework.objectingest.ObjectSourceItem;
import org.pipelineframework.repository.PayloadReference;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

class S3ObjectSourceProviderTest {

    @Test
    void materializesBindingOwnedS3ReferenceWithBoundedStableContent() {
        S3Client client = mock(S3Client.class);
        AtomicInteger executorCalls = new AtomicInteger();
        when(client.headObject(any(software.amazon.awssdk.services.s3.model.HeadObjectRequest.class)))
            .thenReturn(HeadObjectResponse.builder()
                .contentLength(3L)
                .eTag("\"abc123\"")
                .build());
        when(client.getObjectAsBytes(any(GetObjectRequest.class)))
            .thenReturn(ResponseBytes.fromByteArray(
                GetObjectResponse.builder()
                    .contentLength(3L)
                    .contentType("application/pdf")
                    .eTag("\"abc123\"")
                    .build(),
                new byte[] {1, 2, 3}));

        MaterializedPayload materialized = new S3ObjectSourceProvider(client, command -> {
            executorCalls.incrementAndGet();
            command.run();
        })
            .materialize(reference("invoice.pdf", "abc123", 3L), 10L)
            .toCompletableFuture()
            .join();

        assertArrayEquals(new byte[] {1, 2, 3}, materialized.bytes());
        assertEquals("application/pdf", materialized.contentType());
        assertEquals("abc123", materialized.checksum());
        assertEquals(1, executorCalls.get());
    }

    @Test
    void rejectsOversizedReferenceBeforeContactingS3() {
        S3Client client = mock(S3Client.class);

        try (S3ObjectSourceProvider provider = new S3ObjectSourceProvider(client)) {
            assertThrows(CompletionException.class, () -> provider
                .materialize(reference("invoice.pdf", "abc123", 100L), 10L).toCompletableFuture().join());
        }

        verify(client, never()).headObject(any(software.amazon.awssdk.services.s3.model.HeadObjectRequest.class));
        verify(client, never()).getObjectAsBytes(any(GetObjectRequest.class));
    }

    @Test
    void rejectsChangedS3ObjectBeforeDownloadingIt() {
        S3Client client = mock(S3Client.class);
        when(client.headObject(any(software.amazon.awssdk.services.s3.model.HeadObjectRequest.class)))
            .thenReturn(HeadObjectResponse.builder()
                .contentLength(3L)
                .eTag("\"different\"")
                .build());

        try (S3ObjectSourceProvider provider = new S3ObjectSourceProvider(client)) {
            assertThrows(CompletionException.class, () -> provider
                .materialize(reference("invoice.pdf", "abc123", 3L), 10L).toCompletableFuture().join());
        }

        verify(client, never()).getObjectAsBytes(any(GetObjectRequest.class));
    }

    @Test
    void rejectsPayloadWhoseActualBytesExceedTheLimit() {
        S3Client client = mock(S3Client.class);
        when(client.headObject(any(software.amazon.awssdk.services.s3.model.HeadObjectRequest.class)))
            .thenReturn(HeadObjectResponse.builder()
                .contentLength(3L)
                .eTag("\"abc123\"")
                .build());
        when(client.getObjectAsBytes(any(GetObjectRequest.class)))
            .thenReturn(ResponseBytes.fromByteArray(
                GetObjectResponse.builder().eTag("\"abc123\"").build(),
                new byte[] {1, 2, 3, 4}));

        try (S3ObjectSourceProvider provider = new S3ObjectSourceProvider(client)) {
            assertThrows(CompletionException.class, () -> provider
                .materialize(reference("invoice.pdf", "abc123", 3L), 3L).toCompletableFuture().join());
        }
    }

    @Test
    void verifiesPublishedS3ReferenceUsingItsSha256ContentChecksum() throws Exception {
        byte[] bytes = "published".getBytes(StandardCharsets.UTF_8);
        String checksum = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        S3Client client = mock(S3Client.class);
        when(client.headObject(any(software.amazon.awssdk.services.s3.model.HeadObjectRequest.class)))
            .thenReturn(HeadObjectResponse.builder()
                .contentLength((long) bytes.length)
                .eTag("\"s3-etag\"")
                .build());
        when(client.getObjectAsBytes(any(GetObjectRequest.class)))
            .thenReturn(ResponseBytes.fromByteArray(
                GetObjectResponse.builder().eTag("\"s3-etag\"").build(), bytes));
        PayloadReference published = new PayloadReference(
            "s3",
            "docs",
            "published.pdf",
            "application/pdf",
            "raw",
            checksum,
            bytes.length,
            null,
            Map.of(
                "target", "published-documents",
                S3ObjectSourceProvider.CHECKSUM_KIND_METADATA,
                S3ObjectSourceProvider.CHECKSUM_KIND_SHA256),
            Optional.empty());

        MaterializedPayload materialized;
        try (S3ObjectSourceProvider provider = new S3ObjectSourceProvider(client)) {
            materialized = provider.materialize(published, 100L).toCompletableFuture().join();
        }

        assertArrayEquals(bytes, materialized.bytes());
        assertEquals(checksum, materialized.checksum());
    }

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

        List<ObjectSourceItem> items;
        try (S3ObjectSourceProvider provider = new S3ObjectSourceProvider(client)) {
            items = provider.list(source, 10);
        }

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

        List<ObjectSourceItem> items;
        try (S3ObjectSourceProvider provider = new S3ObjectSourceProvider(client)) {
            items = provider.list(source, 1);
        }

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

        try (S3ObjectSourceProvider provider = new S3ObjectSourceProvider(client)) {
            assertThrows(IllegalStateException.class,
                () -> provider.readText(source, item("raw/doc.txt"), 10L));
        }
        verify(client, never()).getObjectAsBytes(any(GetObjectRequest.class));
    }

    @Test
    void rejectsClientResolutionAfterClose() {
        S3ObjectSourceProvider provider = new S3ObjectSourceProvider(mock(S3Client.class));
        PipelineObjectSourceConfig source = new PipelineObjectSourceConfig(
            "search-documents", "object", "s3", Map.of("bucket", "docs"), null, null, null, null);

        provider.close();

        assertThrows(IllegalStateException.class, () -> provider.list(source, 1));
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

    private PayloadReference reference(String key, String etag, long sizeBytes) {
        return new PayloadReference(
            "s3",
            "docs",
            key,
            "application/pdf",
            "raw",
            etag,
            sizeBytes,
            null,
            Map.of(),
            Optional.empty());
    }
}
