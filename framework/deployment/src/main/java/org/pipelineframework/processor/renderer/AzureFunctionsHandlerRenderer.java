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

    @Override
    protected String getCloudProvider() {
        return "azure";
    }

    @Override
    protected ClassName getContextClassName() {
        return EXECUTION_CONTEXT;
    }

    @Override
    protected ClassName getHandlerInterfaceClassName() {
        // Azure Functions doesn't use a marker interface like Lambda
        // Instead, it uses @FunctionName annotation
        return ClassName.OBJECT;
    }

    @Override
    protected String getRequestIdExpression() {
        // Azure Functions ExecutionContext provides getInvocationId() for unique request identification
        return "context != null ? context.getInvocationId() : $S";
    }

    @Override
    protected String getFunctionNameExpression() {
        return "context != null ? context.getFunctionName() : $S";
    }

    @Override
    protected String getExecutionIdExpression() {
        // Azure ExecutionContext provides getInvocationId() for unique request identification
        return "context != null ? context.getInvocationId() : null";
    }

    @Override
    protected String getHandlerSuffix() {
        return "AzureFunction";
    }
}
