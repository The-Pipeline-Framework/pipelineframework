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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PipelineJavaDomainGeneratorTest {

  @TempDir
  Path tempDir;

  @Test
  void generatesFromYmlAndRemovesStaleJavaSources() throws Exception {
    Path sourceConfig = Path.of("..", "..", "examples", "csv-payments", "config", "pipeline.yaml").toRealPath();
    Path moduleDir = tempDir.resolve("module");
    Files.createDirectories(moduleDir);
    Path config = moduleDir.resolve("payments.yml");
    Files.copy(sourceConfig, config);
    Files.copy(sourceConfig.resolveSibling("pipeline.idl.json"), moduleDir.resolve("payments.idl.json"));
    Path outputDir = moduleDir.resolve("generated-domain");
    Path staleSource = outputDir.resolve("stale/RetiredDomain.java");
    Files.createDirectories(staleSource.getParent());
    Files.writeString(staleSource, "class RetiredDomain {}\n");

    new PipelineJavaDomainGenerator().generate(moduleDir, Optional.of(config), Optional.of(outputDir));

    assertFalse(Files.exists(staleSource));
    try (var generatedSources = Files.walk(outputDir)) {
      assertTrue(generatedSources.anyMatch(path -> path.getFileName().toString().endsWith(".java")));
    }
  }
}
