package org.pipelineframework.orchestrator;

import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Locale;

import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * S3-compatible release artifact store for multi-coordinator self-hosted deployments.
 */
public class S3PipelineReleaseArtifactStore implements PipelineReleaseArtifactStore {

    private static final String SHA_256_PREFIX = "sha256:";

    private final S3Client client;
    private final String bucket;
    private final String prefix;

    S3PipelineReleaseArtifactStore(S3Client client, String bucket, String prefix) {
        if (client == null) {
            throw new IllegalArgumentException("S3 client is required");
        }
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalArgumentException(
                "pipeline.orchestrator.releases.storage.s3.bucket must not be blank");
        }
        this.client = client;
        this.bucket = bucket.trim();
        this.prefix = normalizePrefix(prefix);
    }

    @Override
    public PipelineReleaseStoredArtifact store(Path sourcePath) {
        if (sourcePath == null || !Files.isRegularFile(sourcePath) || !Files.isReadable(sourcePath)) {
            throw new IllegalArgumentException("artifactPath must point to a readable release artifact");
        }
        try {
            String checksum = checksum(sourcePath);
            long size = Files.size(sourcePath);
            String key = objectKey(checksum, sourcePath);
            client.putObject(
                PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .metadata(java.util.Map.of(
                        "sha256", stripChecksumPrefix(checksum),
                        "size", Long.toString(size)))
                    .build(),
                RequestBody.fromFile(sourcePath));
            return new PipelineReleaseStoredArtifact(s3Uri(bucket, key), size, checksum);
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to store S3 release artifact: " + e.getMessage(), e);
        }
    }

    @Override
    public void verify(PipelineReleaseRecord record) {
        if (record == null) {
            throw new IllegalArgumentException("Release record is required");
        }
        S3Location location = parse(record.primaryArtifactUri());
        try {
            HeadObjectResponse head = client.headObject(HeadObjectRequest.builder()
                .bucket(location.bucket())
                .key(location.key())
                .build());
            if (record.primaryArtifactSizeBytes() >= 0
                && head.contentLength() != record.primaryArtifactSizeBytes()) {
                throw new IllegalStateException("Stored S3 release artifact size does not match registry metadata");
            }
            String checksum = checksum(client.getObject(GetObjectRequest.builder()
                .bucket(location.bucket())
                .key(location.key())
                .build()));
            if (!checksum.equals(record.primaryArtifactChecksum())) {
                throw new IllegalStateException("Stored S3 release artifact checksum does not match registry metadata");
            }
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw e;
        } catch (S3Exception e) {
            throw new IllegalStateException("Stored S3 release artifact is missing or unreadable", e);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to verify S3 release artifact: " + e.getMessage(), e);
        }
    }

    String bucket() {
        return bucket;
    }

    String prefix() {
        return prefix;
    }

    @Override
    public void close() {
        client.close();
    }

    private String objectKey(String checksum, Path sourcePath) {
        String filename = stripChecksumPrefix(checksum);
        String sourceName = sourcePath.getFileName() == null ? "" : sourcePath.getFileName().toString();
        int dot = sourceName.lastIndexOf('.');
        String extension = dot >= 0 ? sourceName.substring(dot) : ".artifact";
        String key = "sha256/" + filename + extension;
        return prefix.isBlank() ? key : prefix + "/" + key;
    }

    static S3Client newClient(PipelineOrchestratorConfig config) {
        PipelineOrchestratorConfig.ReleaseStorageS3Config s3 = s3Config(config);
        var builder = S3Client.builder()
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .serviceConfiguration(S3Configuration.builder()
                .pathStyleAccessEnabled(s3.pathStyleAccess())
                .build());
        s3.region().filter(value -> !value.isBlank())
            .map(Region::of)
            .ifPresent(builder::region);
        s3.endpointOverride().filter(value -> !value.isBlank())
            .map(URI::create)
            .ifPresent(builder::endpointOverride);
        return builder.build();
    }

    static String bucket(PipelineOrchestratorConfig config) {
        return s3Config(config).bucket()
            .filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalStateException(
                "pipeline.orchestrator.releases.storage.s3.bucket must not be blank"));
    }

    static String prefix(PipelineOrchestratorConfig config) {
        return s3Config(config).prefix();
    }

    private static PipelineOrchestratorConfig.ReleaseStorageS3Config s3Config(PipelineOrchestratorConfig config) {
        if (config == null || config.releases() == null || config.releases().storage() == null
            || config.releases().storage().s3() == null) {
            throw new IllegalStateException(
                "S3 release artifact store requires pipeline.orchestrator.releases.storage.s3.* configuration");
        }
        return config.releases().storage().s3();
    }

    private static S3Location parse(String artifactUri) {
        if (artifactUri == null || artifactUri.isBlank()) {
            throw new IllegalArgumentException("Release artifact URI is required");
        }
        URI uri = URI.create(artifactUri);
        if (!"s3".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null || uri.getHost().isBlank()) {
            throw new IllegalArgumentException("Release artifact URI must use s3://bucket/key");
        }
        String key = uri.getPath() == null ? "" : uri.getPath().replaceFirst("^/", "");
        if (key.isBlank()) {
            throw new IllegalArgumentException("Release artifact URI must include an object key");
        }
        return new S3Location(uri.getHost(), key);
    }

    private static String s3Uri(String bucket, String key) {
        return "s3://" + bucket + "/" + key;
    }

    private static String normalizePrefix(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        return value.trim().replaceAll("^/+", "").replaceAll("/+$", "");
    }

    private static String checksum(Path path) throws Exception {
        try (InputStream input = Files.newInputStream(path)) {
            return checksum(input);
        }
    }

    private static String checksum(InputStream input) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream stream = input) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = stream.read(buffer)) >= 0) {
                if (read > 0) {
                    digest.update(buffer, 0, read);
                }
            }
        }
        return SHA_256_PREFIX + HexFormat.of().formatHex(digest.digest()).toLowerCase(Locale.ROOT);
    }

    private static String stripChecksumPrefix(String checksum) {
        if (checksum == null || checksum.isBlank()) {
            throw new IllegalArgumentException("Artifact checksum is required");
        }
        return checksum.startsWith(SHA_256_PREFIX)
            ? checksum.substring(SHA_256_PREFIX.length())
            : checksum;
    }

    private record S3Location(String bucket, String key) {
    }
}
