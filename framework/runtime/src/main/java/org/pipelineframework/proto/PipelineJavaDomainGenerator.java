/*
 * Copyright (c) 2026 Mariano Barcia
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

package org.pipelineframework.proto;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Writes Java-domain target sources rendered from a shared v3 generation plan. */
final class PipelineJavaDomainGenerator {
    private final PipelineJavaDomainRenderer renderer = new PipelineJavaDomainRenderer();

    void generate(Path outputDirectory, PipelineV3GenerationPlan plan) {
        try {
            List<PipelineJavaDomainRenderer.RenderedSource> sources = renderer.render(plan);
            removeStaleSources(outputDirectory, sources);
            for (PipelineJavaDomainRenderer.RenderedSource source : sources) {
                Path target = outputDirectory.resolve(source.relativePath());
                Files.createDirectories(target.getParent());
                Files.writeString(target, source.content());
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write generated version 3 Java domain sources", e);
        }
    }

    private void removeStaleSources(Path outputDirectory, List<PipelineJavaDomainRenderer.RenderedSource> sources)
        throws IOException {
        Path packageDirectory = outputDirectory.resolve(sources.getFirst().relativePath()).getParent();
        if (!Files.isDirectory(packageDirectory)) {
            return;
        }
        try (var existingSources = Files.list(packageDirectory)) {
            for (Path existing : existingSources.filter(path -> path.getFileName().toString().endsWith(".java")).toList()) {
                Files.delete(existing);
            }
        }
    }
}
