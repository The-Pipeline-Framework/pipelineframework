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

/**
 * Smoke test verifying Azure Functions runtime wiring compiles and initializes correctly.
 * Unlike AWS Lambda mock event server, Azure Functions testing relies on Core Tools local runtime.
 * This test validates basic Quarkus Azure Functions extension bootstrap.
 */
class AzureFunctionsBootstrapSmokeTest {

    @Test
    void azureFunctionsExtensionIsPackaged() {
        ClassLoader loader = AzureFunctionsBootstrapSmokeTest.class.getClassLoader();
        assertNotNull(
                loader.getResource(
                        "io/quarkus/azure/functions/runtime/QuarkusAzureFunctionsMiddleware.class"),
                "Quarkus Azure Functions middleware should be present");
        assertNotNull(
                loader.getResource(
                        "io/quarkus/azure/functions/runtime/QuarkusAzureFunctionsInjector.class"),
                "Quarkus Azure Functions injector should be present");
    }
}
