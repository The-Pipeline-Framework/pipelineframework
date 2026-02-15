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

package org.pipelineframework.processor.phase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import javax.annotation.processing.Messager;
import javax.tools.Diagnostic;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

/** Unit tests for DiscoveryConfigLoader */
@ExtendWith(MockitoExtension.class)
class DiscoveryConfigLoaderTest {

    private final DiscoveryConfigLoader loader = new DiscoveryConfigLoader();

    @Mock
    private Messager messager;

    @TempDir
    Path tempDir;

    // --- Config path resolution ---

    @Test
    void resolvePipelineConfigPath_explicitExistingPath() throws Exception {
        Path configFile = tempDir.resolve("pipeline.yaml");
        Files.writeString(configFile, "pipeline: {}");

        Optional<Path> result = loader.resolvePipelineConfigPath(
            Map.of("pipeline.config", configFile.toString()), tempDir, messager);

        assertTrue(result.isPresent());
        assertEquals(configFile, result.get());
    }

    @Test
    void resolvePipelineConfigPath_explicitMissingPath_emitsError() {
        Optional<Path> result = loader.resolvePipelineConfigPath(
            Map.of("pipeline.config", "/nonexistent/pipeline.yaml"), tempDir, messager);

        assertTrue(result.isEmpty());
        verify(messager).printMessage(eq(Diagnostic.Kind.ERROR), contains("pipeline.config points to a missing path"));
    }

    @Test
    void resolvePipelineConfigPath_explicitRelative_resolvedAgainstModuleDir() throws Exception {
        Path configFile = tempDir.resolve("config/pipeline.yaml");
        Files.createDirectories(configFile.getParent());
        Files.writeString(configFile, "pipeline: {}");

        Optional<Path> result = loader.resolvePipelineConfigPath(
            Map.of("pipeline.config", "config/pipeline.yaml"), tempDir, messager);

        assertTrue(result.isPresent());
        assertEquals(configFile, result.get());
    }

    @Test
    void resolvePipelineConfigPath_noExplicit_noModuleDir_empty() {
        Optional<Path> result = loader.resolvePipelineConfigPath(Map.of(), null, messager);
        assertTrue(result.isEmpty());
    }

    @Test
    void resolvePipelineConfigPath_noExplicit_moduleDirWithNoYaml_empty() {
        Optional<Path> result = loader.resolvePipelineConfigPath(Map.of(), tempDir, messager);
        assertTrue(result.isEmpty());
    }

    // --- Runtime mapping ---

    @Test
    void loadRuntimeMapping_nullModuleDir_returnsNull() {
        assertNull(loader.loadRuntimeMapping(null, messager));
    }

    @Test
    void loadRuntimeMapping_noMappingFile_returnsNull() {
        assertNull(loader.loadRuntimeMapping(tempDir, messager));
    }
}
