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
import com.squareup.javapoet.CodeBlock;

/**
 * Generates Azure Functions HTTP trigger handlers for unary REST resources.
 *
 * <p>The generated handler uses Azure Functions annotations and HTTP trigger bindings.
 * It maps Azure's {@code ExecutionContext} to {@code FunctionTransportContext}.</p>
 *
 * <p>Note: Azure Functions uses HTTP triggers for all cardinalities. The handler
 * routes requests to Quarkus REST endpoints which handle the actual pipeline execution.</p>
 */
public class AzureFunctionsHandlerRenderer extends AbstractFunctionHandlerRenderer {
    private static final ClassName EXECUTION_CONTEXT =
        ClassName.get("com.microsoft.azure.functions", "ExecutionContext");

    /**
     * Creates a new AzureFunctionsHandlerRenderer.
     */
    public AzureFunctionsHandlerRenderer() {
    }

    /**
     * Specifies the cloud provider identifier used by this renderer.
     *
     * @return {@code "azure"} the cloud provider identifier for Azure.
     */
    @Override
    protected String getCloudProvider() {
        return "azure";
    }

    /**
     * Provides the ClassName for the Azure Functions handler context type used in generated code.
     *
     * @return the ClassName representing com.microsoft.azure.functions.ExecutionContext
     */
    @Override
    protected ClassName getContextClassName() {
        return EXECUTION_CONTEXT;
    }

    /**
     * Provide the handler interface class name for Azure Functions handlers.
     *
     * Azure Functions identifies handlers by the {@code @FunctionName} annotation rather than a marker interface,
     * so a generic {@code ClassName.OBJECT} is used.
     *
     * @return {@code ClassName.OBJECT} to indicate no specific handler interface is required
     */
    @Override
    protected ClassName getHandlerInterfaceClassName() {
        // Azure Functions doesn't use a marker interface like Lambda
        // Instead, it uses @FunctionName annotation
        return ClassName.OBJECT;
    }

    /**
     * Produces the Java expression used to obtain the request identifier in an Azure Functions handler.
     *
     * @return the expression {@code context != null ? context.getInvocationId() : $S} which evaluates to the ExecutionContext invocation id when {@code context} is non-null, or a fallback string otherwise.
     */
    @Override
    protected CodeBlock getRequestIdExpression() {
        // Azure Functions ExecutionContext provides getInvocationId() for unique request identification
        return CodeBlock.of("context != null ? context.getInvocationId() : $S", UNKNOWN_REQUEST);
    }

    /**
     * Provide the Java expression used to obtain the Azure Function's name at runtime.
     *
     * @return a String containing the expression `context != null ? context.getFunctionName() : $S` which evaluates to the function name from the Azure `ExecutionContext` when available, or to the provided fallback placeholder otherwise.
     */
    @Override
    protected CodeBlock getFunctionNameExpression() {
        return CodeBlock.of("context != null ? context.getFunctionName() : $S", UNKNOWN_REQUEST);
    }

    /**
     * Expression used in generated handlers to obtain the execution identifier from the Azure ExecutionContext.
     *
     * @return the Java expression string that evaluates to the execution id via {@code context.getInvocationId()} when {@code context} is non-null, or {@code null} otherwise.
     */
    @Override
    protected CodeBlock getExecutionIdExpression() {
        // Azure ExecutionContext provides getInvocationId() for unique request identification
        return CodeBlock.of("context != null ? context.getInvocationId() : null");
    }

    /**
     * Suffix appended to generated handler class names for Azure Functions.
     *
     * @return the handler class name suffix "AzureFunction"
     */
    @Override
    protected String getHandlerSuffix() {
        return "AzureFunction";
    }
}
