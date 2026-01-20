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

package org.pipelineframework.rest;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.core.Response;

import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.pipelineframework.cache.CacheMissException;
import org.pipelineframework.cache.CachePolicyViolation;

/**
 * Default REST exception mapper used by generated resources.
 */
@ApplicationScoped
public class RestExceptionMapper {

    private static final Logger LOG = Logger.getLogger(RestExceptionMapper.class);

    /**
     * Creates a new RestExceptionMapper.
     */
    public RestExceptionMapper() {
    }

    /**
     * Convert exceptions thrown by resources into corresponding HTTP responses.
     *
     * Maps recognised pipeline and framework exceptions to specific HTTP status codes:
     * cache-related exceptions yield 412 Precondition Failed, NotFoundException yields 404 Not Found,
     * IllegalArgumentException yields 400 Bad Request, and all other exceptions yield 500 Internal Server Error.
     *
     * @param ex the exception thrown by a resource or during request processing
     * @return a RestResponse containing an HTTP status and a human-readable error message
     */
    @ServerExceptionMapper
    public RestResponse<String> handleException(Exception ex) {
        if (ex instanceof CacheMissException) {
            LOG.warn("Required cache entry missing", ex);
            return RestResponse.status(Response.Status.PRECONDITION_FAILED, ex.getMessage());
        }
        if (ex instanceof CachePolicyViolation) {
            LOG.warn("Cache policy violation", ex);
            return RestResponse.status(Response.Status.PRECONDITION_FAILED, ex.getMessage());
        }
        if (ex instanceof NotFoundException) {
            LOG.debug("Request did not match a REST endpoint", ex);
            return RestResponse.status(Response.Status.NOT_FOUND, "Not Found");
        }
        if (ex instanceof IllegalArgumentException) {
            LOG.warn("Invalid request", ex);
            return RestResponse.status(Response.Status.BAD_REQUEST, "Invalid request");
        }
        LOG.error("Unexpected error processing request", ex);
        return RestResponse.status(Response.Status.INTERNAL_SERVER_ERROR, "An unexpected error occurred");
    }
}