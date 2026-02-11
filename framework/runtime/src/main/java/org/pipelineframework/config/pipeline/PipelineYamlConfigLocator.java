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
import java.util.Objects;
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
     * Locate the pipeline configuration file for a module by searching the module first and falling back to the nearest parent Maven project.
     *
     * Searches the following locations in order: the module directory, moduleDir/config, and moduleDir/src/main/resources. If none are found, it locates the nearest ancestor directory that contains a pom.xml with packaging "pom" and searches that project root and its config subdirectory.
     *
     * @param moduleDir the module directory to search from; must not be null
     * @return an Optional containing the matched pipeline configuration file path, or empty if no match is found
     * @throws IllegalStateException if multiple candidate pipeline configuration files are found
     */
    public Optional<Path> locate(Path moduleDir) {
        Objects.requireNonNull(moduleDir, "moduleDir must not be null");
        List<Path> moduleMatches = new ArrayList<>();
        scanDirectory(moduleDir, moduleMatches);
        scanDirectory(moduleDir.resolve("config"), moduleMatches);
        scanDirectory(moduleDir.resolve("src").resolve("main").resolve("resources"), moduleMatches);
        if (!moduleMatches.isEmpty()) {
            validateSingleMatch(moduleMatches);
            return Optional.of(moduleMatches.get(0));
        }

        Path projectRoot = findNearestParentPom(moduleDir);
        if (projectRoot == null) {
            return Optional.empty();
        }

        List<Path> matches = new ArrayList<>();
        scanDirectory(projectRoot, matches);
        scanDirectory(projectRoot.resolve("config"), matches);

        validateSingleMatch(matches);

        return matches.isEmpty() ? Optional.empty() : Optional.of(matches.get(0));
    }

    /**
     * Ensures at most one pipeline configuration file is present among the provided matches.
     *
     * @param matches list of candidate paths for pipeline configuration files
     * @throws IllegalStateException if more than one candidate is present; the exception message lists the conflicting paths
     */
    private void validateSingleMatch(List<Path> matches) {
        if (matches.size() > 1) {
            List<String> namesList = matches.stream()
                .map(Path::toString)
                .sorted()
                .toList();
            String names = String.join(", ", namesList);
            throw new IllegalStateException("Multiple pipeline config files found: " + names);
        }
    }

    /**
     * Locate the nearest ancestor directory (including the given directory) that contains a pom.xml
     * whose <packaging> element is "pom".
     *
     * @param moduleDir the directory to start searching upward from
     * @return the ancestor directory Path whose pom.xml contains "<packaging>pom</packaging>", or
     *         null if no such ancestor is found
     */
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