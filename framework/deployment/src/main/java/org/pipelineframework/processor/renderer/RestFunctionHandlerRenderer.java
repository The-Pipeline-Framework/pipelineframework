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
     * Creates a new RestFunctionHandlerRenderer (AWS Lambda-specific).
     */
    public RestFunctionHandlerRenderer() {
        super();
    }

    /**
     * Returns the generated REST function handler fully-qualified class name.
     * <p>
     * <strong>Deprecated:</strong> This method is retained for backwards compatibility only.
     *
     * @param servicePackage service package
     * @param generatedName generated service name
     * @return handler FQCN
     * @deprecated Use {@link AwsLambdaFunctionHandlerRenderer} directly
     */
    @Deprecated
    @Override
    public String handlerFqcn(String servicePackage, String generatedName) {
        return super.handlerFqcn(servicePackage, generatedName);
    }
}
