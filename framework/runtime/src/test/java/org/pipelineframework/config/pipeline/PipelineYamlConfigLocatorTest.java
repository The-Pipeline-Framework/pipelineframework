package org.pipelineframework.config.pipeline;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;

class PipelineYamlConfigLocatorTest {

    @TempDir
    Path tempDir;

    @Test
    void prefersModuleLocalPipelineConfigOverParentConfig() throws IOException {
        Path parent = createPomProject(tempDir.resolve("parent"));
        Path moduleDir = createModule(parent.resolve("service-a"));

        Path modulePipeline = moduleDir.resolve("pipeline.yaml");
        Files.writeString(modulePipeline, "appName: \"module\"\n");

        Path parentConfigDir = parent.resolve("config");
        Files.createDirectories(parentConfigDir);
        Files.writeString(parentConfigDir.resolve("pipeline.yaml"), "appName: \"parent\"\n");

        PipelineYamlConfigLocator locator = new PipelineYamlConfigLocator();
        Path resolved = locator.locate(moduleDir).orElseThrow();

        assertEquals(modulePipeline, resolved);
    }

    @Test
    void fallsBackToParentConfigWhenModuleConfigMissing() throws IOException {
        Path parent = createPomProject(tempDir.resolve("parent"));
        Path moduleDir = createModule(parent.resolve("service-a"));

        Path parentConfigDir = parent.resolve("config");
        Files.createDirectories(parentConfigDir);
        Path parentPipeline = parentConfigDir.resolve("pipeline.yaml");
        Files.writeString(parentPipeline, "appName: \"parent\"\n");

        PipelineYamlConfigLocator locator = new PipelineYamlConfigLocator();
        Path resolved = locator.locate(moduleDir).orElseThrow();

        assertEquals(parentPipeline, resolved);
    }

    @Test
    void returnsEmptyWhenNoConfigExists() throws IOException {
        Path parent = createPomProject(tempDir.resolve("parent"));
        Path moduleDir = createModule(parent.resolve("service-a"));

        PipelineYamlConfigLocator locator = new PipelineYamlConfigLocator();
        assertTrue(locator.locate(moduleDir).isEmpty());
    }

    private Path createPomProject(Path dir) throws IOException {
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("pom.xml"), """
            <project>
              <modelVersion>4.0.0</modelVersion>
              <groupId>test</groupId>
              <artifactId>parent</artifactId>
              <version>1.0.0</version>
              <packaging>pom</packaging>
            </project>
            """);
        return dir;
    }

    private Path createModule(Path moduleDir) throws IOException {
        Files.createDirectories(moduleDir);
        Files.writeString(moduleDir.resolve("pom.xml"), """
            <project>
              <modelVersion>4.0.0</modelVersion>
              <groupId>test</groupId>
              <artifactId>module</artifactId>
              <version>1.0.0</version>
              <packaging>jar</packaging>
            </project>
            """);
        return moduleDir;
    }
}
