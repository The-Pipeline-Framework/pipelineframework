/*
 * Copyright (c) 2023-2026 Mariano Barcia
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

package org.pipelineframework.search.orchestrator.service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.ZipFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Smoke test verifying Azure Functions runtime wiring compiles and initializes correctly.
 * Unlike AWS Lambda mock event server, Azure Functions testing relies on Core Tools local runtime.
 * This test validates basic Quarkus Azure Functions extension bootstrap.
 */
@EnabledIfSystemProperty(named = "azure.functions.artifact.required", matches = "true")
class AzureFunctionsBootstrapSmokeTest {

    private static final Path AZURE_FUNCTIONS_OUTPUT = Path.of("target", "azure-functions");
    private static final List<String> REQUIRED_ENTRIES =
            List.of(
                    "io/quarkus/azure/functions/runtime/QuarkusAzureFunctionsMiddleware.class",
                    "io/quarkus/azure/functions/runtime/QuarkusAzureFunctionsInjector.class");

    @Test
    void azureFunctionsExtensionIsPackaged() throws IOException {
        assertTrue(
                Files.isDirectory(AZURE_FUNCTIONS_OUTPUT),
                () -> "Azure Functions package directory should exist: " + AZURE_FUNCTIONS_OUTPUT);

        for (String entry : REQUIRED_ENTRIES) {
            assertTrue(
                    packagedOutputContains(entry),
                    () -> "Azure Functions package should contain " + entry);
        }
    }

    private boolean packagedOutputContains(String entry) throws IOException {
        try (var paths = Files.walk(AZURE_FUNCTIONS_OUTPUT)) {
            for (Path path : paths.filter(Files::isRegularFile).toList()) {
                String relativePath = AZURE_FUNCTIONS_OUTPUT.relativize(path).toString().replace('\\', '/');
                if (relativePath.endsWith(entry) || isArchive(path) && archiveContains(path, entry)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isArchive(Path path) {
        String filename = path.getFileName().toString();
        return filename.endsWith(".jar") || filename.endsWith(".zip");
    }

    private boolean archiveContains(Path archive, String entry) throws IOException {
        try (ZipFile zip = new ZipFile(archive.toFile())) {
            return zip.getEntry(entry) != null;
        }
    }
}
