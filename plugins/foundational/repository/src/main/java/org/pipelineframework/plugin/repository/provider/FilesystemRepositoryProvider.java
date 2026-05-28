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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pipelineframework.annotation.ParallelismHint;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.repository.PayloadReference;
import org.pipelineframework.repository.RepositoryChecksums;
import org.pipelineframework.repository.RepositoryProvider;
import org.pipelineframework.repository.RepositoryReadResult;
import org.pipelineframework.repository.RepositoryWriteRequest;

@ApplicationScoped
@Unremovable
@IfBuildProperty(name = "pipeline.repository.provider", stringValue = "filesystem")
@ParallelismHint(ordering = OrderingRequirement.RELAXED, threadSafety = ThreadSafety.SAFE)
public class FilesystemRepositoryProvider implements RepositoryProvider {

    @ConfigProperty(name = "pipeline.repository.filesystem.root", defaultValue = "target/tpf-repository")
    String root;

    @ConfigProperty(name = "pipeline.repository.verify-checksum", defaultValue = "true")
    boolean verifyChecksum;

    @Override
    public String providerName() {
        return "filesystem";
    }

    @Override
    public Uni<PayloadReference> store(RepositoryWriteRequest request) {
        return Uni.createFrom().item(() -> {
            Path path = pathFor(request.container(), request.key());
            try {
                Files.createDirectories(path.getParent());
                Files.write(path, request.payload());
            } catch (IOException e) {
                throw new IllegalStateException("Failed writing repository payload " + path, e);
            }
            return new PayloadReference(
                providerName(),
                request.container(),
                request.key(),
                request.contentType(),
                request.codec(),
                request.checksum(),
                request.payload().length,
                request.version(),
                request.metadata());
        });
    }

    @Override
    public Uni<RepositoryReadResult> load(PayloadReference reference) {
        return Uni.createFrom().item(() -> {
            Path path = pathFor(reference.container(), reference.key());
            byte[] bytes;
            try {
                bytes = Files.readAllBytes(path);
            } catch (IOException e) {
                throw new IllegalStateException("Failed reading repository payload " + path, e);
            }
            if (verifyChecksum && reference.checksum() != null) {
                String actual = RepositoryChecksums.sha256Hex(bytes);
                if (!reference.checksum().equalsIgnoreCase(actual)) {
                    throw new IllegalStateException("Repository payload checksum mismatch for " + reference.key());
                }
            }
            return new RepositoryReadResult(reference, bytes, reference.contentType(), reference.codec(), reference.checksum());
        });
    }

    @Override
    public Uni<Boolean> exists(PayloadReference reference) {
        return Uni.createFrom().item(() -> Files.exists(pathFor(reference.container(), reference.key())));
    }

    @Override
    public Uni<Boolean> delete(PayloadReference reference) {
        return Uni.createFrom().item(() -> {
            try {
                return Files.deleteIfExists(pathFor(reference.container(), reference.key()));
            } catch (IOException e) {
                throw new IllegalStateException("Failed deleting repository payload " + reference.key(), e);
            }
        });
    }

    private Path pathFor(String container, String key) {
        Path base = Path.of(root).toAbsolutePath().normalize();
        Path resolved = (container == null ? base : base.resolve(container)).resolve(key).normalize();
        if (!resolved.startsWith(base)) {
            throw new IllegalArgumentException("Repository key escapes filesystem root: " + key);
        }
        return resolved;
    }
}
