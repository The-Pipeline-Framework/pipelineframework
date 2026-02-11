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

import jakarta.inject.Inject;
import jakarta.inject.Named;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

/**
 * Default cache invalidation Lambda entrypoint that delegates to the generated raw-document side-effect handler.
 */
@Named("CacheInvalidationFunctionHandler")
public class CacheInvalidationFunctionHandler implements RequestHandler<Object, Object> {

    @Inject
    @Named("CacheRawDocumentSideEffectFunctionHandler")
    RequestHandler<Object, Object> delegate;

    @Override
    public Object handleRequest(Object input, Context context) {
        return delegate.handleRequest(input, context);
    }
}
