package org.pipelineframework.config.pipeline;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

    @Test
    void throwsWhenModuleDirIsNull() {
        PipelineYamlConfigLocator locator = new PipelineYamlConfigLocator();
        assertThrows(NullPointerException.class, () -> locator.locate(null));
    }

    @Test
    void returnsEmptyForNonExistentModuleDir() {
        PipelineYamlConfigLocator locator = new PipelineYamlConfigLocator();
        Path missing = tempDir.resolve("does-not-exist").resolve("module-a");
        assertTrue(locator.locate(missing).isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {"pipeline.yaml", "pipeline.yml", "pipeline-config.yaml"})
    void locatesExactPipelineConfigNamesFromClasspath(String filename) throws IOException {
        Files.writeString(tempDir.resolve(filename), "appName: classpath\n");

        try (URLClassLoader classLoader = isolatedClassLoader(tempDir)) {
            URL resolved = new PipelineYamlConfigLocator().locateResource(classLoader).orElseThrow();

            assertEquals(filename, Path.of(resolved.getPath()).getFileName().toString());
        }
    }

    @Test
    void locatesCanvasPipelineConfigFromClasspath() throws IOException {
        Files.writeString(tempDir.resolve("search-canvas-config.yaml"), "appName: classpath\n");

        try (URLClassLoader classLoader = isolatedClassLoader(tempDir)) {
            URL resolved = new PipelineYamlConfigLocator().locateResource(classLoader).orElseThrow();

            assertEquals("search-canvas-config.yaml", Path.of(resolved.getPath()).getFileName().toString());
        }
    }

    @Test
    void locatesCanvasPipelineConfigFromManifestFreePackagedClasspath() throws IOException {
        Path jar = createJarWithResource("search-canvas-config.yaml");

        try (URLClassLoader classLoader = new URLClassLoader(new URL[]{jar.toUri().toURL()}, null)) {
            URL resolved = new PipelineYamlConfigLocator().locateResource(classLoader).orElseThrow();

            assertEquals("jar", resolved.getProtocol());
            assertTrue(resolved.toExternalForm().endsWith("!/search-canvas-config.yaml"));
        }
    }

    @Test
    void resolvesConfigFromGrandparentProject() throws IOException {
        Path grandparent = createPomProject(tempDir.resolve("grandparent"));
        Path grandparentConfigDir = grandparent.resolve("config");
        Files.createDirectories(grandparentConfigDir);
        Path grandparentPipeline = grandparentConfigDir.resolve("pipeline.yaml");
        Files.writeString(grandparentPipeline, "appName: \"grandparent\"\n");

        Path parent = grandparent.resolve("parent");
        Files.createDirectories(parent);
        Files.writeString(parent.resolve("pom.xml"), """
            <project>
              <modelVersion>4.0.0</modelVersion>
              <groupId>test</groupId>
              <artifactId>parent</artifactId>
              <version>1.0.0</version>
              <packaging>jar</packaging>
            </project>
            """);

        Path moduleDir = createModule(parent.resolve("service-a").resolve("nested-module"));

        PipelineYamlConfigLocator locator = new PipelineYamlConfigLocator();
        Path resolved = locator.locate(moduleDir).orElseThrow();

        assertEquals(grandparentPipeline, resolved);
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

    private URLClassLoader isolatedClassLoader(Path root) throws IOException {
        return new URLClassLoader(new URL[]{root.toUri().toURL()}, null);
    }

    private Path createJarWithResource(String resourceName) throws IOException {
        Path jar = tempDir.resolve("application.jar");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            output.putNextEntry(new JarEntry(resourceName));
            output.write("appName: classpath\n".getBytes(StandardCharsets.UTF_8));
            output.closeEntry();
        }
        return jar;
    }
}
