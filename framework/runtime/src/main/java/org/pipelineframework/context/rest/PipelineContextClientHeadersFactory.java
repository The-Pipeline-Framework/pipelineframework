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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.MultivaluedMap;

import io.quarkus.arc.Unremovable;
import org.eclipse.microprofile.rest.client.ext.ClientHeadersFactory;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHeaders;
import org.pipelineframework.context.PipelineContextHolder;

/**
 * Propagates pipeline context headers on REST client requests.
 */
@ApplicationScoped
@Unremovable
public class PipelineContextClientHeadersFactory implements ClientHeadersFactory {

    /**
     * Creates a new PipelineContextClientHeadersFactory.
     */
    public PipelineContextClientHeadersFactory() {
    }

    @Override
    public MultivaluedMap<String, String> update(
            MultivaluedMap<String, String> incomingHeaders,
            MultivaluedMap<String, String> clientOutgoingHeaders) {
        if (clientOutgoingHeaders == null) {
            return clientOutgoingHeaders;
        }
        String version = headerValue(incomingHeaders, PipelineContextHeaders.VERSION);
        String replay = headerValue(incomingHeaders, PipelineContextHeaders.REPLAY);
        String cachePolicy = headerValue(incomingHeaders, PipelineContextHeaders.CACHE_POLICY);

        if (version == null || replay == null || cachePolicy == null) {
            PipelineContext context = PipelineContextHolder.get();
            if (context != null) {
                if (version == null) {
                    version = context.versionTag();
                }
                if (replay == null) {
                    replay = context.replayMode();
                }
                if (cachePolicy == null) {
                    cachePolicy = context.cachePolicy();
                }
            }
        }

        putIfPresent(clientOutgoingHeaders, PipelineContextHeaders.VERSION, version);
        putIfPresent(clientOutgoingHeaders, PipelineContextHeaders.REPLAY, replay);
        putIfPresent(clientOutgoingHeaders, PipelineContextHeaders.CACHE_POLICY, cachePolicy);
        return clientOutgoingHeaders;
    }

    private String headerValue(MultivaluedMap<String, String> headers, String name) {
        if (headers == null || name == null) {
            return null;
        }
        String value = headers.getFirst(name);
        if (value != null && !value.isBlank()) {
            return value;
        }
        for (String key : headers.keySet()) {
            if (key != null && key.equalsIgnoreCase(name)) {
                String candidate = headers.getFirst(key);
                if (candidate != null && !candidate.isBlank()) {
                    return candidate;
                }
            }
        }
        return null;
    }

    private void putIfPresent(
            MultivaluedMap<String, String> headers,
            String name,
            String value) {
        if (value != null && !value.isBlank()) {
            headers.putSingle(name, value);
        }
    }
}
