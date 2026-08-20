package org.pipelineframework.connector.objectingest;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
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

    private final Optional<S3Client> client;
    private final boolean ownsClient;
    private final Executor executor;
    private final boolean ownsExecutor;
    private final int partSizeBytes;
    private final ConcurrentMap<ClientConfiguration, S3Client> resolvedClients = new ConcurrentHashMap<>();
    private final Object lifecycleLock = new Object();
    private boolean closed;

    public S3ObjectTargetProvider() {
        this(Optional.empty(), true, Executors.newVirtualThreadPerTaskExecutor(), true, DEFAULT_PART_SIZE_BYTES);
    }

    public S3ObjectTargetProvider(S3Client client) {
        this(Optional.of(Objects.requireNonNull(client, "client")), false, Executors.newVirtualThreadPerTaskExecutor(), true,
            DEFAULT_PART_SIZE_BYTES);
    }

    S3ObjectTargetProvider(S3Client client, Executor executor, int partSizeBytes) {
        this(Optional.of(Objects.requireNonNull(client, "client")), false, executor, false, partSizeBytes);
    }

    private S3ObjectTargetProvider(
        Optional<S3Client> client,
        boolean ownsClient,
        Executor executor,
        boolean ownsExecutor,
        int partSizeBytes
    ) {
        this.client = client;
        this.ownsClient = ownsClient;
        this.executor = executor;
        this.ownsExecutor = ownsExecutor;
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
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            closed = true;
            if (ownsClient) {
                resolvedClients.values().forEach(S3Client::close);
                resolvedClients.clear();
            }
        }
        if (ownsExecutor && executor instanceof ExecutorService executorService) {
            executorService.close();
        }
    }

    private S3Client client(ObjectWriteOpenRequest request) {
        rejectEndpointOverride(request);
        synchronized (lifecycleLock) {
            if (closed) {
                throw new IllegalStateException("S3 object target provider is closed");
            }
            return client.orElseGet(() -> resolvedClient(request));
        }
    }

    private S3Client resolvedClient(ObjectWriteOpenRequest request) {
        ClientConfiguration configuration = new ClientConfiguration(
            optional(request, "region").orElse(""),
            Boolean.parseBoolean(optional(request, "pathStyleAccess").orElse("false")));
        return resolvedClients.computeIfAbsent(configuration, configured -> {
            S3ClientBuilder builder = S3Client.builder().httpClientBuilder(UrlConnectionHttpClient.builder());
            if (!configured.region().isBlank()) {
                builder.region(Region.of(configured.region()));
            }
            if (configured.pathStyleAccess()) {
                builder.serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build());
            }
            return builder.build();
        });
    }

    private void rejectEndpointOverride(ObjectWriteOpenRequest request) {
        if (optional(request, "endpoint").isPresent() || optional(request, "endpointOverride").isPresent()) {
            throw new IllegalArgumentException("S3 endpoint overrides must be configured through an application-provided S3Client");
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
        return location(request.target(), key);
    }

    private static Optional<String> location(PipelineObjectPublishConfig target, String key) {
        Object value = target.location().get(key);
        return value == null || value.toString().isBlank()
            ? Optional.empty()
            : Optional.of(value.toString().trim());
    }

    private record ClientConfiguration(String region, boolean pathStyleAccess) {
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
        private final MessageDigest digest = sha256Digest();
        private long writtenBytes;
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
                digest.update(bytes);
                writtenBytes += bytes.length;
                buffer.writeBytes(bytes);
                while (buffer.size() >= partSizeBytes) {
                    uploadBufferedPart(partSizeBytes);
                }
            }, executor);
        }

        @Override
        public CompletionStage<ObjectWriteResult> close(ObjectWriteCloseRequest closeRequest) {
            return CompletableFuture.supplyAsync(() -> {
                String actualChecksum = HexFormat.of().formatHex(digest.digest());
                if (closeRequest.bytes() != writtenBytes) {
                    throw new IllegalStateException("S3 written byte count mismatch: expected "
                        + closeRequest.bytes() + " but wrote " + writtenBytes);
                }
                if (closeRequest.checksum() != null
                        && !closeRequest.checksum().equalsIgnoreCase(actualChecksum)) {
                    throw new IllegalStateException("S3 written payload checksum mismatch");
                }
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
                metadata.put(
                    S3ObjectSourceProvider.CHECKSUM_KIND_METADATA,
                    S3ObjectSourceProvider.CHECKSUM_KIND_SHA256);
                location(request.target(), "region")
                    .ifPresent(region -> metadata.put(S3ObjectSourceProvider.REGION_METADATA, region));
                PayloadReference reference = new PayloadReference(
                    "s3",
                    bucket,
                    key,
                    request.contentType(),
                    "raw",
                    actualChecksum,
                    writtenBytes,
                    null,
                    metadata,
                    Optional.empty());
                return new ObjectWriteResult(reference, writtenBytes, actualChecksum, Instant.now());
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

        private static MessageDigest sha256Digest() {
            try {
                return MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException failure) {
                throw new IllegalStateException("SHA-256 is unavailable", failure);
            }
        }
    }
}
