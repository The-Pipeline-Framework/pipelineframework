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
package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class GrpcCodegenConfigurationBoundaryTest {

    private static final String PROTOCOL_SCAN_PROPERTY = "quarkus.generate-code.grpc.scan-for-proto";

    @Test
    void doesNotShipBuildTimeProtocolScanningAsApplicationConfiguration() throws Exception {
        var runtimeClasses = Path.of(PipelineExecutionService.class
            .getProtectionDomain()
            .getCodeSource()
            .getLocation()
            .toURI());
        var applicationProperties = runtimeClasses.resolve("application.properties");

        assertFalse(load(applicationProperties).containsKey(PROTOCOL_SCAN_PROPERTY),
            "Quarkus protocol scanning must remain local to the runtime module build");
    }

    private Properties load(Path path) throws IOException {
        var properties = new Properties();
        try (var input = Files.newInputStream(path)) {
            properties.load(input);
        }
        return properties;
    }
}
