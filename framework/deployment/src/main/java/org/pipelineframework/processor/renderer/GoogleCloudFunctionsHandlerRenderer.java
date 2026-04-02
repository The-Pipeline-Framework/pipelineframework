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

package org.pipelineframework.processor.renderer;

import com.squareup.javapoet.ClassName;

/**
 * Generates Google Cloud Functions HTTP handlers for unary REST resources.
 *
 * <p>The generated handler implements {@code com.google.cloud.functions.HttpFunction}
 * and uses GCP's HTTP request/response model for metadata extraction.</p>
 *
 * <p>Note: Google Cloud Functions uses HTTP triggers for all cardinalities. The handler
 * routes requests to Quarkus REST endpoints which handle the actual pipeline execution.</p>
 */
public class GoogleCloudFunctionsHandlerRenderer extends AbstractFunctionHandlerRenderer {
    private static final ClassName HTTP_FUNCTION =
        ClassName.get("com.google.cloud.functions", "HttpFunction");
    private static final ClassName HTTP_REQUEST =
        ClassName.get("com.google.cloud.functions", "HttpRequest");

    /**
     * Creates a new GoogleCloudFunctionsHandlerRenderer.
     */
    public GoogleCloudFunctionsHandlerRenderer() {
    }

    @Override
    protected String getCloudProvider() {
        return "gcp";
    }

    @Override
    protected ClassName getContextClassName() {
        // GCP HttpFunction doesn't have a context object like Lambda/Azure
        // We use HttpRequest for metadata extraction
        return HTTP_REQUEST;
    }

    @Override
    protected ClassName getHandlerInterfaceClassName() {
        return HTTP_FUNCTION;
    }

    @Override
    protected String getRequestIdExpression() {
        // Extract trace ID from X-Cloud-Trace-Context header if present, otherwise generate UUID
        // Format: "TRACE_ID/SPAN_ID;o=TRACE_TRUE"
        // Use getFirstHeader() which returns Optional<String>
        return "$T.ofNullable(request.getFirstHeader($S).orElse(null)).orElseGet(() -> $T.randomUUID().toString())";
    }

    @Override
    protected String getFunctionNameExpression() {
        // GCP function name is available via environment variable
        return "System.getenv($S)";
    }

    @Override
    protected String getExecutionIdExpression() {
        // Use request ID from header or generate one
        return "request.getFirstHeader($S).orElseGet(() -> $T.randomUUID().toString())";
    }

    @Override
    protected String getHandlerSuffix() {
        return "GcpFunction";
    }
}
