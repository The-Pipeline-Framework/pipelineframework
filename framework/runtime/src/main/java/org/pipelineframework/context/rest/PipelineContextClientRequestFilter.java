/*
 * Copyright (c) 2023-2025 Mariano Barcia
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

package org.pipelineframework.context.rest;

import jakarta.ws.rs.ConstrainedTo;
import jakarta.ws.rs.RuntimeType;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientRequestFilter;
import jakarta.ws.rs.ext.Provider;

import io.quarkus.arc.Unremovable;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHeaders;
import org.pipelineframework.context.PipelineContextHolder;

/**
 * Propagates pipeline context headers on REST client requests.
 */
@Provider
@ConstrainedTo(RuntimeType.CLIENT)
@Unremovable
public class PipelineContextClientRequestFilter implements ClientRequestFilter {

    /**
     * Creates a new PipelineContextClientRequestFilter.
     */
    public PipelineContextClientRequestFilter() {
    }

    @Override
    public void filter(ClientRequestContext requestContext) {
        PipelineContext context = PipelineContextHolder.get();
        if (context == null) {
            return;
        }
        putIfPresent(requestContext, PipelineContextHeaders.VERSION, context.versionTag());
        putIfPresent(requestContext, PipelineContextHeaders.REPLAY, context.replayMode());
        putIfPresent(requestContext, PipelineContextHeaders.CACHE_POLICY, context.cachePolicy());
    }

    private void putIfPresent(ClientRequestContext requestContext, String name, String value) {
        if (value != null && !value.isBlank()) {
            requestContext.getHeaders().putSingle(name, value);
        }
    }
}
