package org.pipelineframework.connector.objectingest;

import java.net.URI;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.objectpublish.ObjectTargetProvider;
import org.pipelineframework.objectpublish.ObjectWriteRequest;
import org.pipelineframework.objectpublish.ObjectWriteResult;
import org.pipelineframework.repository.PayloadReference;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

/**
 * Plain AWS SDK S3 object target provider for Object Publish.
 */
public class S3ObjectTargetProvider implements ObjectTargetProvider, AutoCloseable {

    private final S3Client client;
    private final boolean ownsClient;
    private volatile S3Client resolvedClient;

    public S3ObjectTargetProvider() {
        this.client = null;
        this.ownsClient = true;
    }

    S3ObjectTargetProvider(S3Client client) {
        this.client = client;
        this.ownsClient = false;
    }

    @Override
    public String providerName() {
        return "s3";
    }

    @Override
    public Uni<ObjectWriteResult> write(ObjectWriteRequest request) {
        return Uni.createFrom().item(() -> writeBlocking(request));
    }

    @Override
    public void close() {
        if (ownsClient && resolvedClient != null) {
            resolvedClient.close();
        }
    }

    private ObjectWriteResult writeBlocking(ObjectWriteRequest request) {
        String bucket = required(request, "bucket");
        Map<String, String> metadata = new LinkedHashMap<>(request.metadata());
        metadata.put("target", request.targetName());
        PutObjectResponse response = client(request).putObject(PutObjectRequest.builder()
            .bucket(bucket)
            .key(request.objectKey())
            .contentType(request.contentType())
            .metadata(metadata)
            .build(), RequestBody.fromBytes(request.bytes()));
        String version = response.versionId();
        PayloadReference reference = new PayloadReference(
            providerName(),
            bucket,
            request.objectKey(),
            request.contentType(),
            "raw",
            request.checksum(),
            request.bytes().length,
            version,
            metadata);
        return new ObjectWriteResult(reference, request.bytes().length, request.checksum(), Instant.now());
    }

    private S3Client client(ObjectWriteRequest request) {
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
            optional(request, "region").ifPresent(region -> builder.region(Region.of(region)));
            optional(request, "endpoint").map(URI::create).ifPresent(builder::endpointOverride);
            resolvedClient = builder.build();
            return resolvedClient;
        }
    }

    private String required(ObjectWriteRequest request, String key) {
        return optional(request, key)
            .orElseThrow(() -> new IllegalArgumentException(
                "s3 publish target '" + request.targetName() + "' requires location." + key));
    }

    private Optional<String> optional(ObjectWriteRequest request, String key) {
        Object value = request.target().location().get(key);
        return value == null || value.toString().isBlank()
            ? Optional.empty()
            : Optional.of(value.toString().trim());
    }
}
