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

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CsvPaymentsHaTopologyConfigTest {

  @TempDir
  Path tempDir;

  @Test
  void generatesContainerSqsTopologyAgainstTheCanonicalIdl() throws Exception {
    generatesAgainstCanonicalIdl("pipeline.container-sqs.yaml");
  }

  @Test
  void generatesObjectIngestTopologyAgainstTheCanonicalIdl() throws Exception {
    generatesAgainstCanonicalIdl("pipeline.object-ingest.yaml");
  }

  private void generatesAgainstCanonicalIdl(String configName) throws Exception {
    Path configDirectory = Path.of("..", "..", "examples", "csv-payments", "config").toRealPath();
    Path moduleDir = tempDir.resolve(configName);
    Files.createDirectories(moduleDir);
    Path config = moduleDir.resolve("pipeline.yaml");
    Files.copy(configDirectory.resolve(configName), config);
    Files.copy(configDirectory.resolve("pipeline.idl.json"), moduleDir.resolve("pipeline.idl.json"));

    new PipelineProtoGenerator().generate(moduleDir, config, moduleDir.resolve("generated"));
  }
}
