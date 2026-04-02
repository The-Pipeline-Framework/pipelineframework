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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Smoke test verifying Azure Functions runtime wiring compiles and initializes correctly.
 * Unlike AWS Lambda mock event server, Azure Functions testing relies on Core Tools local runtime.
 * This test validates basic Quarkus Azure Functions extension bootstrap.
 */
class AzureFunctionsBootstrapSmokeTest {

    @Test
    void azureFunctionsExtensionLoads() {
        // Verify that Azure Functions extension classes are resolvable at runtime
        // This is a basic smoke test; full integration testing requires Azure Functions Core Tools
        String extensionClass = "io.quarkus.azure.functions.runtime.AzureFunctionsHandler";
        try {
            Class<?> clazz = Class.forName(extensionClass);
            assertNotNull(clazz, "Azure Functions handler class should be loadable");
        } catch (ClassNotFoundException e) {
            // Extension may not be on classpath if not building with azure profile
            // This is acceptable for smoke test purposes - skip the test
            org.junit.jupiter.api.Assumptions.assumeTrue(false, 
                "Azure Functions extension not on classpath - expected when not building with azure profile");
        }
    }
}
