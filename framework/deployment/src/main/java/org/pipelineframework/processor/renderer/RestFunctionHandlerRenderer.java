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

/**
 * Backwards-compatible wrapper that delegates to {@link AwsLambdaFunctionHandlerRenderer}.
 *
 * <p>Historically, this class generated AWS Lambda RequestHandler wrappers. It now delegates
 * to the dedicated AWS Lambda renderer for backwards compatibility.</p>
 *
 * @deprecated Use {@link AwsLambdaFunctionHandlerRenderer} directly, or use the
 * {@link FunctionHandlerRendererFactory} for multi-cloud support.
 */
@Deprecated
public class RestFunctionHandlerRenderer extends AwsLambdaFunctionHandlerRenderer {
    /**
     * Constructs a deprecated RestFunctionHandlerRenderer used for backwards compatibility with legacy REST handler generation.
     *
     * @deprecated Use {@link AwsLambdaFunctionHandlerRenderer} instead.
     */
    public RestFunctionHandlerRenderer() {
        super();
    }

    /**
     * Get the fully-qualified class name of the generated REST function handler.
     *
     * This method is retained for backwards compatibility.
     *
     * @param servicePackage the service package name
     * @param generatedName the generated service name
     * @return the handler's fully-qualified class name
     * @deprecated Use {@link AwsLambdaFunctionHandlerRenderer} directly
     */
    @Deprecated
    @Override
    public String handlerFqcn(String servicePackage, String generatedName) {
        return super.handlerFqcn(servicePackage, generatedName);
    }
}
