package org.pipelineframework.connector.objectingest;

import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.pipelineframework.config.boundary.PipelineObjectSourceConfig;
import org.pipelineframework.objectingest.ObjectSourceItem;
import org.pipelineframework.objectingest.ObjectSourceProvider;
import org.pipelineframework.repository.PayloadReference;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Plain AWS SDK S3 object source provider.
 */
public class S3ObjectSourceProvider implements ObjectSourceProvider, AutoCloseable {

    private final S3Client client;
    private final boolean ownsClient;
    private volatile S3Client resolvedClient;

    public S3ObjectSourceProvider() {
        this.client = null;
        this.ownsClient = true;
    }

    S3ObjectSourceProvider(S3Client client) {
        this.client = client;
        this.ownsClient = false;
    }

    @Override
    public String providerName() {
        return "s3";
    }

    @Override
    public List<ObjectSourceItem> list(PipelineObjectSourceConfig source, int limit) {
        String bucket = required(source, "bucket");
        String prefix = optional(source, "prefix").orElse("");
        int requested = Math.max(1, limit);
        List<ObjectSourceItem> matches = new ArrayList<>();
        String continuationToken = null;
        do {
            ListObjectsV2Response response = client(source).listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .maxKeys(requested)
                .continuationToken(continuationToken)
                .build());
            for (S3Object item : response.contents()) {
                if (matches(source, item.key())) {
                    matches.add(item(source, bucket, item));
                    if (matches.size() == requested) {
                        return List.copyOf(matches);
                    }
                }
            }
            continuationToken = response.nextContinuationToken();
        } while (continuationToken != null);
        return List.copyOf(matches);
    }

    @Override
    public Optional<String> readText(PipelineObjectSourceConfig source, ObjectSourceItem item, long maxBytes) {
        String bucket = required(source, "bucket");
        Long contentLength = client(source).headObject(HeadObjectRequest.builder()
                .bucket(bucket)
                .key(item.key())
                .build())
            .contentLength();
        if (maxBytes > 0 && contentLength == null) {
            throw new IllegalStateException(
                "Cannot enforce maxBytes limit: S3 object contentLength unavailable for " + item.key());
        }
        if (maxBytes > 0 && contentLength != null && contentLength > maxBytes) {
            throw new IllegalStateException("Object exceeds configured maxBytes: " + item.key());
        }
        ResponseBytes<GetObjectResponse> bytes = client(source).getObjectAsBytes(GetObjectRequest.builder()
            .bucket(bucket)
            .key(item.key())
            .build());
        byte[] payload = bytes.asByteArray();
        if (maxBytes > 0 && payload.length > maxBytes) {
            throw new IllegalStateException("Object exceeds configured maxBytes: " + item.key());
        }
        return Optional.of(new String(payload, source.payload().charset()));
    }

    @Override
    public void close() {
        if (ownsClient && resolvedClient != null) {
            resolvedClient.close();
        }
    }

    private ObjectSourceItem item(PipelineObjectSourceConfig source, String bucket, S3Object item) {
        String etag = normalizeEtag(item.eTag());
        long size = item.size() == null ? 0L : item.size();
        long lastModified = item.lastModified() == null ? 0L : item.lastModified().toEpochMilli();
        PayloadReference reference = new PayloadReference(
            providerName(),
            bucket,
            item.key(),
            null,
            "raw",
            etag,
            size,
            null,
            Map.of("source", source.name()));
        return new ObjectSourceItem(
            providerName(),
            bucket,
            item.key(),
            null,
            etag,
            size,
            lastModified,
            null,
            Map.of(),
            reference,
            null);
    }

    private boolean matches(PipelineObjectSourceConfig source, String key) {
        boolean included = source.filter().include().isEmpty()
            || source.filter().include().stream().anyMatch(pattern -> globMatches(pattern, key));
        boolean excluded = source.filter().exclude().stream().anyMatch(pattern -> globMatches(pattern, key));
        return included && !excluded;
    }

    private S3Client client(PipelineObjectSourceConfig source) {
        if (client != null) {
            return client;
        }
        S3Client local = resolvedClient;
        if (local != null) {
            return local;
        }
        synchronized (this) {
            local = resolvedClient;
            if (local != null) {
                return local;
            }
            S3ClientBuilder builder = S3Client.builder().httpClientBuilder(UrlConnectionHttpClient.builder());
            optional(source, "region").ifPresent(region -> builder.region(Region.of(region)));
            optional(source, "endpoint").map(URI::create).ifPresent(builder::endpointOverride);
            resolvedClient = builder.build();
            return resolvedClient;
        }
    }

    private boolean globMatches(String pattern, String key) {
        return FileSystems.getDefault().getPathMatcher("glob:" + pattern).matches(Path.of(key));
    }

    private String required(PipelineObjectSourceConfig source, String key) {
        return optional(source, key)
            .orElseThrow(() -> new IllegalArgumentException("s3 source '" + source.name() + "' requires location." + key));
    }

    private Optional<String> optional(PipelineObjectSourceConfig source, String key) {
        Object value = source.location().get(key);
        return value == null || value.toString().isBlank()
            ? Optional.empty()
            : Optional.of(value.toString().trim());
    }

    private String normalizeEtag(String etag) {
        if (etag == null || etag.isBlank()) {
            return null;
        }
        return etag.replace("\"", "").trim();
    }
}
