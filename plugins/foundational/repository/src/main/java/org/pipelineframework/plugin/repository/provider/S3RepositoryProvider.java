/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.plugin.repository.provider;

import java.net.URI;
import java.util.Optional;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.quarkus.arc.Unremovable;
import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pipelineframework.annotation.ParallelismHint;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.repository.PayloadReference;
import org.pipelineframework.repository.PayloadNotFoundException;
import org.pipelineframework.repository.RepositoryChecksums;
import org.pipelineframework.repository.RepositoryProvider;
import org.pipelineframework.repository.RepositoryReadResult;
import org.pipelineframework.repository.RepositoryWriteRequest;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

@ApplicationScoped
@Unremovable
@IfBuildProperty(name = "pipeline.repository.provider", stringValue = "s3")
@ParallelismHint(ordering = OrderingRequirement.RELAXED, threadSafety = ThreadSafety.SAFE)
public class S3RepositoryProvider implements RepositoryProvider {

    @Inject
    S3Client injectedClient;

    @ConfigProperty(name = "pipeline.repository.s3.bucket")
    String bucket;

    @ConfigProperty(name = "pipeline.repository.s3.prefix", defaultValue = "")
    String prefix;

    @ConfigProperty(name = "pipeline.repository.s3.region")
    Optional<String> region;

    @ConfigProperty(name = "pipeline.repository.s3.endpoint-override")
    Optional<String> endpointOverride;

    @ConfigProperty(name = "pipeline.repository.s3.path-style", defaultValue = "false")
    boolean pathStyle;

    @ConfigProperty(name = "pipeline.repository.verify-checksum", defaultValue = "true")
    boolean verifyChecksum;

    private S3Client client;

    @PostConstruct
    void init() {
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalStateException("pipeline.repository.s3.bucket must be configured when using S3 repository provider");
        }
        if (injectedClient != null) {
            client = injectedClient;
            return;
        }
        var builder = S3Client.builder();
        region.map(Region::of).ifPresent(builder::region);
        endpointOverride.map(URI::create).ifPresent(builder::endpointOverride);
        builder.forcePathStyle(pathStyle);
        client = builder.build();
    }

    @Override
    public String providerName() {
        return "s3";
    }

    @Override
    public Uni<PayloadReference> store(RepositoryWriteRequest request) {
        return Uni.createFrom().item(() -> {
            String key = s3Key(request.key());
            String targetBucket = request.container() == null || request.container().isBlank() ? bucket : request.container();
            PutObjectRequest put = PutObjectRequest.builder()
                .bucket(targetBucket)
                .key(key)
                .contentType(request.contentType())
                .metadata(request.metadata())
                .build();
            client.putObject(put, RequestBody.fromBytes(request.payload()));
            return new PayloadReference(
                providerName(),
                targetBucket,
                key,
                request.contentType(),
                request.codec(),
                request.checksum(),
                request.payload().length,
                request.version(),
                request.metadata());
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<RepositoryReadResult> load(PayloadReference reference) {
        return Uni.createFrom().item(() -> {
            ResponseBytes<GetObjectResponse> response;
            try {
                response = client.getObjectAsBytes(builder -> builder
                    .bucket(resolveBucket(reference))
                    .key(reference.key()));
            } catch (NoSuchKeyException e) {
                throw new PayloadNotFoundException(reference, e);
            } catch (S3Exception e) {
                if (e.statusCode() == 404) {
                    throw new PayloadNotFoundException(reference, e);
                }
                throw e;
            }
            byte[] bytes = response.asByteArray();
            if (verifyChecksum && reference.checksum() != null) {
                String actual = RepositoryChecksums.sha256Hex(bytes);
                if (!reference.checksum().equalsIgnoreCase(actual)) {
                    throw new IllegalStateException("Repository payload checksum mismatch for " + reference.key());
                }
            }
            return new RepositoryReadResult(reference, bytes, reference.contentType(), reference.codec(), reference.checksum());
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<Boolean> exists(PayloadReference reference) {
        return Uni.createFrom().item(() -> {
            try {
                client.headObject(HeadObjectRequest.builder()
                    .bucket(resolveBucket(reference))
                    .key(reference.key())
                    .build());
                return true;
            } catch (NoSuchKeyException e) {
                return false;
            } catch (S3Exception e) {
                if (e.statusCode() == 404) {
                    return false;
                }
                throw e;
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private String resolveBucket(PayloadReference reference) {
        return reference.container() == null ? bucket : reference.container();
    }

    private String s3Key(String key) {
        String normalizedPrefix = prefix == null ? "" : prefix.strip();
        if (normalizedPrefix.isBlank()) {
            return key;
        }
        String cleanPrefix = normalizedPrefix.endsWith("/") ? normalizedPrefix : normalizedPrefix + "/";
        return cleanPrefix + key;
    }
}
