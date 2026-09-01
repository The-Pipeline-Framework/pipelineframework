package org.pipelineframework.processor.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PipelineRuntimeMappingLocatorTest {

    @TempDir
    Path tempDir;

    private final PipelineRuntimeMappingLocator locator = new PipelineRuntimeMappingLocator();

    @Test
    void moduleLocalMappingOverridesAggregatorMapping() throws Exception {
        Path moduleDir = module("query");
        Path localMapping = Files.writeString(moduleDir.resolve("pipeline.runtime.yaml"), "layout: monolith\n");
        Files.writeString(tempDir.resolve("pipeline.runtime.yaml"), "layout: modular\n");

        assertEquals(localMapping, locator.locate(moduleDir).orElseThrow());
    }

    @Test
    void fallsBackToAggregatorMappingWhenModuleHasNone() throws Exception {
        Path moduleDir = module("query");
        Path sharedMapping = Files.writeString(tempDir.resolve("pipeline.runtime.yaml"), "layout: modular\n");

        assertEquals(sharedMapping, locator.locate(moduleDir).orElseThrow());
    }

    @Test
    void rejectsAmbiguousModuleLocalMappings() throws Exception {
        Path moduleDir = module("query");
        Files.createDirectories(moduleDir.resolve("config"));
        Files.writeString(moduleDir.resolve("pipeline.runtime.yaml"), "layout: monolith\n");
        Files.writeString(moduleDir.resolve("config/pipeline.runtime.yml"), "layout: monolith\n");

        assertThrows(IllegalStateException.class, () -> locator.locate(moduleDir));
    }

    private Path module(String name) throws Exception {
        Files.writeString(tempDir.resolve("pom.xml"), """
            <project>
              <modelVersion>4.0.0</modelVersion>
              <groupId>example</groupId>
              <artifactId>parent</artifactId>
              <version>1</version>
              <packaging>pom</packaging>
            </project>
            """);
        Path moduleDir = Files.createDirectories(tempDir.resolve(name));
        Files.writeString(moduleDir.resolve("pom.xml"), """
            <project>
              <modelVersion>4.0.0</modelVersion>
              <groupId>example</groupId>
              <artifactId>module</artifactId>
              <version>1</version>
            </project>
            """);
        return moduleDir;
    }
}
