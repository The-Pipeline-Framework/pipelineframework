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

package org.pipelineframework.search.cache_invalidation.service.pipeline;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.inject.Named;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.pipelineframework.search.lambda.NoopLambdaHandler;

/**
 * Default cache invalidation Lambda entrypoint that delegates to the generated raw-document side-effect handler.
 */
@Named("CacheInvalidationFunctionHandler")
public class CacheInvalidationFunctionHandler implements RequestHandler<Object, Object> {
    private static final String DELEGATE_HANDLER_NAME = "CacheRawDocumentSideEffectFunctionHandler";

    @Override
    public Object handleRequest(Object input, Context context) {
        return resolveDelegate().handleRequest(input, context);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private RequestHandler<Object, Object> resolveDelegate() {
        Instance<RequestHandler> namedHandlers = CDI.current()
            .select(RequestHandler.class, NamedLiteral.of(DELEGATE_HANDLER_NAME));
        if (namedHandlers.isResolvable()) {
            return namedHandlers.get();
        }

        Instance<NoopLambdaHandler> noopHandlers = CDI.current().select(NoopLambdaHandler.class);
        if (noopHandlers.isResolvable()) {
            return noopHandlers.get();
        }

        return new NoopLambdaHandler();
    }
}
