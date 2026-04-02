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

    /**
     * Identifies the target cloud provider for generated handlers.
     *
     * @return the cloud provider identifier "gcp"
     */
    @Override
    protected String getCloudProvider() {
        return "gcp";
    }

    /**
     * Identify the context class used for extracting request metadata on Google Cloud Functions.
     *
     * @return the ClassName representing com.google.cloud.functions.HttpRequest used as the context
     */
    @Override
    protected ClassName getContextClassName() {
        // GCP HttpFunction doesn't have a context object like Lambda/Azure
        // We use HttpRequest for metadata extraction
        return HTTP_REQUEST;
    }

    /**
     * Provide the handler interface used for generated Google Cloud Functions HTTP handlers.
     *
     * @return the ClassName representing com.google.cloud.functions.HttpFunction
     */
    @Override
    protected ClassName getHandlerInterfaceClassName() {
        return HTTP_FUNCTION;
    }

    /**
     * Provide the code-generation expression that extracts a request identifier from the GCP HttpRequest.
     *
     * The expression reads the "X-Cloud-Trace-Context" header and uses its value when present; otherwise it falls back to a randomly generated UUID.
     *
     * @return a templated Java expression string that resolves to the request ID
     */
    @Override
    protected String getRequestIdExpression() {
        // Extract trace ID from X-Cloud-Trace-Context header if present, otherwise generate UUID
        // Format: "TRACE_ID/SPAN_ID;o=TRACE_TRUE"
        // Use getFirstHeader() which returns Optional<String>
        return "$T.ofNullable(request.getFirstHeader($S).orElse(null)).orElseGet(() -> $T.randomUUID().toString())";
    }

    /**
     * Provide a Java expression that reads the Cloud Function name from the environment.
     *
     * @return a String containing a Java expression that invokes System.getenv with a string placeholder (`$S`) to obtain the function name
     */
    @Override
    protected String getFunctionNameExpression() {
        // GCP function name is available via environment variable
        return "System.getenv($S)";
    }

    /**
     * Produce a Java expression that obtains an execution identifier from the incoming request or generates one.
     *
     * @return a string expression which reads the `X-Cloud-Trace-Context` header via `request.getFirstHeader(...)`
     *         and falls back to `UUID.randomUUID().toString()` when the header is absent
     */
    @Override
    protected String getExecutionIdExpression() {
        // Use request ID from header or generate one
        return "request.getFirstHeader($S).orElseGet(() -> $T.randomUUID().toString())";
    }

    /**
     * Suffix used when forming the generated handler class name for Google Cloud Functions.
     *
     * @return the handler name suffix "GcpFunction".
     */
    @Override
    protected String getHandlerSuffix() {
        return "GcpFunction";
    }
}
