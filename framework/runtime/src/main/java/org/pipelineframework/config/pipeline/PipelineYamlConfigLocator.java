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

package org.pipelineframework.config.pipeline;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Locates pipeline.yaml configuration files starting from a module directory.
 */
public class PipelineYamlConfigLocator {

    /**
     * Creates a new PipelineYamlConfigLocator.
     */
    public PipelineYamlConfigLocator() {
    }

    private static final List<String> EXACT_FILENAMES = List.of(
        "pipeline.yaml",
        "pipeline.yml",
        "pipeline-config.yaml"
    );

    /**
     * Locate the pipeline configuration file for the given module.
     *
     * @param moduleDir the module directory to search from
     * @return the resolved config path if found
     */
    public Optional<Path> locate(Path moduleDir) {
        Path projectRoot = findNearestParentPom(moduleDir);
        if (projectRoot == null) {
            return Optional.empty();
        }

        List<Path> matches = new ArrayList<>();
        scanDirectory(projectRoot, matches);
        scanDirectory(projectRoot.resolve("config"), matches);

        if (matches.size() > 1) {
            String names = matches.stream()
                .map(Path::toString)
                .sorted()
                .reduce((a, b) -> a + ", " + b)
                .orElse("");
            throw new IllegalStateException("Multiple pipeline config files found: " + names);
        }

        return matches.isEmpty() ? Optional.empty() : Optional.of(matches.get(0));
    }

    private Path findNearestParentPom(Path moduleDir) {
        Path current = moduleDir;
        while (current != null) {
            Path pomPath = current.resolve("pom.xml");
            if (Files.isRegularFile(pomPath) && isPomPackagingPom(pomPath)) {
                return current;
            }
            current = current.getParent();
        }
        return null;
    }

    private boolean isPomPackagingPom(Path pomPath) {
        try {
            String content = Files.readString(pomPath);
            return content.contains("<packaging>pom</packaging>");
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read pom.xml at " + pomPath, e);
        }
    }

    private void scanDirectory(Path directory, List<Path> matches) {
        if (!Files.isDirectory(directory)) {
            return;
        }

        try (var stream = Files.list(directory)) {
            stream.filter(Files::isRegularFile)
                .forEach(path -> {
                    String filename = path.getFileName().toString();
                    if (matchesPipelineFilename(filename)) {
                        matches.add(path);
                    }
                });
        } catch (IOException e) {
            throw new IllegalStateException("Failed to scan pipeline config directory: " + directory, e);
        }
    }

    private boolean matchesPipelineFilename(String filename) {
        if (EXACT_FILENAMES.contains(filename)) {
            return true;
        }
        return filename.endsWith("-canvas-config.yaml");
    }
}
