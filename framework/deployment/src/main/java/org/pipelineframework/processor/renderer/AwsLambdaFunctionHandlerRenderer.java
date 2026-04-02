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
 * Generates AWS Lambda RequestHandler wrappers for unary REST resources.
 *
 * <p>The generated handler implements {@code com.amazonaws.services.lambda.runtime.RequestHandler}
 * and uses AWS Lambda's {@code Context} for metadata extraction.</p>
 */
public class AwsLambdaFunctionHandlerRenderer extends AbstractFunctionHandlerRenderer {
    private static final ClassName LAMBDA_CONTEXT =
        ClassName.get("com.amazonaws.services.lambda.runtime", "Context");
    private static final ClassName REQUEST_HANDLER =
        ClassName.get("com.amazonaws.services.lambda.runtime", "RequestHandler");

    /**
     * Creates a new AwsLambdaFunctionHandlerRenderer.
     */
    public AwsLambdaFunctionHandlerRenderer() {
    }

    @Override
    protected String getCloudProvider() {
        return "aws";
    }

    @Override
    protected ClassName getContextClassName() {
        return LAMBDA_CONTEXT;
    }

    @Override
    protected ClassName getHandlerInterfaceClassName() {
        return REQUEST_HANDLER;
    }

    @Override
    protected String getRequestIdExpression() {
        return "context != null ? context.getAwsRequestId() : $S";
    }

    @Override
    protected String getFunctionNameExpression() {
        return "context != null ? context.getFunctionName() : $S";
    }

    @Override
    protected String getExecutionIdExpression() {
        return "context != null ? context.getLogStreamName() : $S";
    }

    @Override
    protected String getHandlerSuffix() {
        return "FunctionHandler";
    }

    @Override
    protected String buildTransportContextStatement() {
        // AWS Lambda uses specific context methods for metadata
        // Note: This returns a JavaPoet format string - caller must supply arguments for $T/$S placeholders
        return "$T transportContext = $T.of(" +
            "context != null ? context.getAwsRequestId() : $S, " +
            "context != null ? context.getFunctionName() : $S, " +
            "$S, " +
            "$T.of(" +
            "$T.ATTR_CORRELATION_ID, context != null ? context.getAwsRequestId() : $S, " +
            "$T.ATTR_EXECUTION_ID, (context != null && context.getLogStreamName() != null " +
            "&& !context.getLogStreamName().isBlank()) ? context.getLogStreamName() : $T.randomUUID().toString(), " +
            "$T.ATTR_RETRY_ATTEMPT, $T.getProperty($S, $S), " +
            "$T.ATTR_DISPATCH_TS_EPOCH_MS, $T.toString($T.currentTimeMillis())))";
    }
}
