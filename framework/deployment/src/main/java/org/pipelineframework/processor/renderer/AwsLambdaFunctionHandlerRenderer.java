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

    /**
     * Identifies the cloud provider for this renderer.
     *
     * @return the cloud provider identifier "aws"
     */
    @Override
    protected String getCloudProvider() {
        return "aws";
    }

    /**
     * Provide the JavaPoet ClassName for the AWS Lambda Context type.
     *
     * @return the JavaPoet `ClassName` for `com.amazonaws.services.lambda.runtime.Context`
     */
    @Override
    protected ClassName getContextClassName() {
        return LAMBDA_CONTEXT;
    }

    /**
     * Provides the AWS Lambda RequestHandler interface ClassName used for generated handlers.
     *
     * @return the ClassName for AWS Lambda's com.amazonaws.services.lambda.runtime.RequestHandler.
     */
    @Override
    protected ClassName getHandlerInterfaceClassName() {
        return REQUEST_HANDLER;
    }

    /**
     * Provides a JavaPoet format string that selects the AWS request ID from the Lambda {@code Context} or a placeholder when the context is null.
     *
     * @return A format string that evaluates to {@code context.getAwsRequestId()} when {@code context != null}, otherwise the {@code $S} placeholder.
     */
    @Override
    protected String getRequestIdExpression() {
        return "context != null ? context.getAwsRequestId() : $S";
    }

    /**
     * Provide the JavaPoet expression that yields the AWS Lambda function name from the Lambda Context or a placeholder.
     *
     * @return a format string that resolves to `context.getFunctionName()` when `context` is non-null, otherwise the string placeholder `$S`
     */
    @Override
    protected String getFunctionNameExpression() {
        return "context != null ? context.getFunctionName() : $S";
    }

    /**
     * Provides the JavaPoet format string used to obtain the execution identifier from the Lambda Context.
     *
     * <p>The returned format string selects {@code context.getLogStreamName()} when {@code context} is non-null; otherwise it uses the string placeholder {@code $S}.</p>
     *
     * @return the JavaPoet format string {@code "context != null ? context.getLogStreamName() : $S"}
     */
    @Override
    protected String getExecutionIdExpression() {
        return "context != null ? context.getLogStreamName() : $S";
    }

    /**
     * Suffix appended to generated handler class names for AWS Lambda RequestHandler wrappers.
     *
     * @return the suffix string "FunctionHandler" used when naming generated handler classes
     */
    @Override
    protected String getHandlerSuffix() {
        return "FunctionHandler";
    }

    /**
     * Builds a JavaPoet format string that constructs an AWS-specific `transportContext`.
     *
     * <p>The returned format string, when supplied with the required `$T`/`$S` arguments and formatted into generated code,
     * produces an expression that creates a `transportContext` populated from Lambda `Context` values when available
     * and sensible fallbacks otherwise.
     *
     * @return a JavaPoet format string which (once type/string placeholders are provided) generates code that:
     *         - sets the correlation id from `context.getAwsRequestId()` or from the provided `$S` fallback,
     *         - sets the function name from `context.getFunctionName()` or from the provided `$S` fallback,
     *         - sets the execution id to `context.getLogStreamName()` when non-null and not blank, otherwise to `UUID.randomUUID().toString()`,
     *         - includes a retry-attempt value obtained via a property lookup placeholder,
     *         - includes a dispatch timestamp based on the current epoch millis;
     *         callers must supply the matching `$T` and `$S` arguments required by the format string.
     */
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
