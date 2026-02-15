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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for DiscoveryPathResolver */
class DiscoveryPathResolverTest {

    private final DiscoveryPathResolver resolver = new DiscoveryPathResolver();

    @TempDir
    Path tempDir;

    @Test
    void resolveGeneratedSourcesRoot_explicitDir() {
        Path result = resolver.resolveGeneratedSourcesRoot(Map.of("pipeline.generatedSourcesDir", "/custom/path"));
        assertEquals(Paths.get("/custom/path"), result);
    }

    @Test
    void resolveGeneratedSourcesRoot_fallbackRoot() {
        Path result = resolver.resolveGeneratedSourcesRoot(Map.of("pipeline.generatedSourcesRoot", "/fallback"));
        assertEquals(Paths.get("/fallback"), result);
    }

    @Test
    void resolveGeneratedSourcesRoot_dirTakesPrecedence() {
        Path result = resolver.resolveGeneratedSourcesRoot(Map.of(
            "pipeline.generatedSourcesDir", "/primary",
            "pipeline.generatedSourcesRoot", "/fallback"
        ));
        assertEquals(Paths.get("/primary"), result);
    }

    @Test
    void resolveGeneratedSourcesRoot_default() {
        Path result = resolver.resolveGeneratedSourcesRoot(Map.of());
        Path expected = Paths.get(System.getProperty("user.dir"), "target", "generated-sources", "pipeline");
        assertEquals(expected, result);
    }

    @Test
    void resolveGeneratedSourcesRoot_blankIgnored() {
        Path result = resolver.resolveGeneratedSourcesRoot(Map.of("pipeline.generatedSourcesDir", "  "));
        Path expected = Paths.get(System.getProperty("user.dir"), "target", "generated-sources", "pipeline");
        assertEquals(expected, result);
    }

    @Test
    void resolveModuleDir_fromGeneratedSourcesRoot() {
        // Simulates .../target/generated-sources/pipeline -> module root
        Path genRoot = tempDir.resolve("target/generated-sources/pipeline");
        Path result = resolver.resolveModuleDir(genRoot);
        assertEquals(tempDir, result);
    }

    @Test
    void resolveModuleDir_nullFallsBackToCwd() {
        Path result = resolver.resolveModuleDir(null);
        assertEquals(Paths.get(System.getProperty("user.dir")), result);
    }

    @Test
    void resolveModuleName_present() {
        assertEquals("my-module", resolver.resolveModuleName(Map.of("pipeline.module", "my-module")));
    }

    @Test
    void resolveModuleName_trimmed() {
        assertEquals("my-module", resolver.resolveModuleName(Map.of("pipeline.module", "  my-module  ")));
    }

    @Test
    void resolveModuleName_missing() {
        assertNull(resolver.resolveModuleName(Map.of()));
    }

    @Test
    void resolveModuleName_blank() {
        assertNull(resolver.resolveModuleName(Map.of("pipeline.module", "  ")));
    }
}
