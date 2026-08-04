package org.pipelineframework.connector.objectingest;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import org.pipelineframework.objectpublish.ObjectTargetProvider;
import org.pipelineframework.objectpublish.ObjectWriteCloseRequest;
import org.pipelineframework.objectpublish.ObjectWriteOpenRequest;
import org.pipelineframework.objectpublish.ObjectWriteResult;
import org.pipelineframework.objectpublish.ObjectWriteSession;
import org.pipelineframework.repository.PayloadReference;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

/**
 * Plain AWS SDK S3 object target provider for Object Publish.
 */
public class S3ObjectTargetProvider implements ObjectTargetProvider, AutoCloseable {
    static final int DEFAULT_PART_SIZE_BYTES = 8 * 1024 * 1024;
    private static final int MIN_PART_SIZE_BYTES = 5 * 1024 * 1024;

    private final S3Client client;
    private final boolean ownsClient;
    private final Executor executor;
    private final int partSizeBytes;
    private volatile S3Client resolvedClient;

    public S3ObjectTargetProvider() {
        this(null, true, ForkJoinPool.commonPool(), DEFAULT_PART_SIZE_BYTES);
    }

    S3ObjectTargetProvider(S3Client client) {
        this(client, false, ForkJoinPool.commonPool(), DEFAULT_PART_SIZE_BYTES);
    }

    S3ObjectTargetProvider(S3Client client, Executor executor, int partSizeBytes) {
        this(client, false, executor, partSizeBytes);
    }

    private S3ObjectTargetProvider(S3Client client, boolean ownsClient, Executor executor, int partSizeBytes) {
        this.client = client;
        this.ownsClient = ownsClient;
        this.executor = executor;
        this.partSizeBytes = Math.max(partSizeBytes, MIN_PART_SIZE_BYTES);
    }

    @Override
    public String providerName() {
        return "s3";
    }

    @Override
    public CompletionStage<ObjectWriteSession> open(ObjectWriteOpenRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            String bucket = required(request, "bucket");
            String key = objectKey(request);
            S3Client s3 = client(request);
            CreateMultipartUploadResponse response = s3.createMultipartUpload(CreateMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType(request.contentType())
                .metadata(request.metadata())
                .build());
            return new S3WriteSession(request, s3, bucket, key, response.uploadId(), executor, partSizeBytes);
        }, executor);
    }

    @Override
    public void close() {
        if (ownsClient && resolvedClient != null) {
            resolvedClient.close();
        }
    }

    private S3Client client(ObjectWriteOpenRequest request) {
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
            optional(request, "endpoint").or(() -> optional(request, "endpointOverride"))
                .map(URI::create)
                .ifPresent(builder::endpointOverride);
            boolean pathStyle = Boolean.parseBoolean(optional(request, "pathStyleAccess").orElse("false"));
            if (pathStyle) {
                builder.serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build());
            }
            resolvedClient = builder.build();
            return resolvedClient;
        }
    }

    private String objectKey(ObjectWriteOpenRequest request) {
        String prefix = optional(request, "prefix").orElse("");
        if (prefix.isBlank()) {
            return request.objectKey();
        }
        String normalizedPrefix = prefix.endsWith("/") ? prefix.substring(0, prefix.length() - 1) : prefix;
        String normalizedKey = request.objectKey().startsWith("/") ? request.objectKey().substring(1) : request.objectKey();
        return normalizedPrefix + "/" + normalizedKey;
    }

    private String required(ObjectWriteOpenRequest request, String key) {
        return optional(request, key)
            .orElseThrow(() -> new IllegalArgumentException(
                "s3 publish target '" + request.targetName() + "' requires location." + key));
    }

    private Optional<String> optional(ObjectWriteOpenRequest request, String key) {
        Object value = request.target().location().get(key);
        return value == null || value.toString().isBlank()
            ? Optional.empty()
            : Optional.of(value.toString().trim());
    }

    private static final class S3WriteSession implements ObjectWriteSession {
        private final ObjectWriteOpenRequest request;
        private final S3Client client;
        private final String bucket;
        private final String key;
        private final String uploadId;
        private final Executor executor;
        private final int partSizeBytes;
        private final List<CompletedPart> parts = new ArrayList<>();
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private int nextPartNumber = 1;
        private boolean completed;

        private S3WriteSession(
            ObjectWriteOpenRequest request,
            S3Client client,
            String bucket,
            String key,
            String uploadId,
            Executor executor,
            int partSizeBytes
        ) {
            this.request = request;
            this.client = client;
            this.bucket = bucket;
            this.key = key;
            this.uploadId = uploadId;
            this.executor = executor;
            this.partSizeBytes = partSizeBytes;
        }

        @Override
        public CompletionStage<Void> write(ByteBuffer chunk) {
            byte[] bytes = copy(chunk);
            return CompletableFuture.runAsync(() -> {
                buffer.writeBytes(bytes);
                while (buffer.size() >= partSizeBytes) {
                    uploadBufferedPart(partSizeBytes);
                }
            }, executor);
        }

        @Override
        public CompletionStage<ObjectWriteResult> close(ObjectWriteCloseRequest closeRequest) {
            return CompletableFuture.supplyAsync(() -> {
                if (buffer.size() > 0) {
                    uploadBufferedPart(buffer.size());
                }
                if (parts.isEmpty()) {
                    client.abortMultipartUpload(abortRequest());
                    client.putObject(
                        PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .contentType(request.contentType())
                            .metadata(request.metadata())
                            .build(),
                        RequestBody.empty());
                } else {
                    client.completeMultipartUpload(
                        CompleteMultipartUploadRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .uploadId(uploadId)
                            .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build())
                            .build());
                }
                completed = true;
                Map<String, String> metadata = new LinkedHashMap<>(request.metadata());
                metadata.putAll(closeRequest.metadata());
                metadata.put("target", request.targetName());
                PayloadReference reference = new PayloadReference(
                    "s3",
                    bucket,
                    key,
                    request.contentType(),
                    "raw",
                    closeRequest.checksum(),
                    closeRequest.bytes(),
                    null,
                    metadata);
                return new ObjectWriteResult(reference, closeRequest.bytes(), closeRequest.checksum(), Instant.now());
            }, executor);
        }

        @Override
        public CompletionStage<Void> abort(Throwable cause) {
            return CompletableFuture.runAsync(() -> {
                if (!completed) {
                    client.abortMultipartUpload(abortRequest());
                    completed = true;
                }
            }, executor);
        }

        private void uploadBufferedPart(int size) {
            byte[] payload = buffer.toByteArray();
            byte[] part = java.util.Arrays.copyOf(payload, size);
            buffer.reset();
            if (payload.length > size) {
                buffer.writeBytes(java.util.Arrays.copyOfRange(payload, size, payload.length));
            }
            int partNumber = nextPartNumber++;
            UploadPartResponse response = client.uploadPart(
                UploadPartRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .uploadId(uploadId)
                    .partNumber(partNumber)
                    .contentLength((long) part.length)
                    .build(),
                RequestBody.fromBytes(part));
            parts.add(CompletedPart.builder().partNumber(partNumber).eTag(response.eTag()).build());
        }

        private AbortMultipartUploadRequest abortRequest() {
            return AbortMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(key)
                .uploadId(uploadId)
                .build();
        }

        private static byte[] copy(ByteBuffer chunk) {
            if (chunk == null) {
                return new byte[0];
            }
            ByteBuffer duplicate = chunk.slice();
            byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            return bytes;
        }
    }
}
