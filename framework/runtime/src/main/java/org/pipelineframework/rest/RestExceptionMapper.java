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
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;

import com.google.rpc.Status;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.pipelineframework.cache.CacheMissException;
import org.pipelineframework.cache.CachePolicyViolation;
import org.pipelineframework.context.TransportDispatchMetadata;
import org.pipelineframework.context.TransportDispatchMetadataHolder;
import org.pipelineframework.transport.http.ProtobufHttpContentTypes;
import org.pipelineframework.transport.http.ProtobufHttpStatusMapper;

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
     * Converts exceptions thrown by resources into corresponding HTTP responses.
     *
     * If the request accepts Protobuf (based on the Accept or Content-Type headers) this returns
     * a JAX-RS {@link javax.ws.rs.core.Response} whose entity is a protobuf-encoded
     * {@link com.google.rpc.Status}. Otherwise maps specific exceptions to REST-friendly responses:
     * CacheMissException and CachePolicyViolation -> 412 Precondition Failed; NotFoundException -> 404 Not Found;
     * an underlying IllegalArgumentException -> 400 Bad Request; all other errors -> 500 Internal Server Error.
     *
     * @param ex the exception thrown by a resource or during request processing
     * @param headers the incoming request headers used to determine response content type (may be null)
     * @return either a JAX-RS {@link javax.ws.rs.core.Response} with a protobuf Status payload,
     *         or a {@code RestResponse<String>} carrying an HTTP status and a human-readable message
     */
    @ServerExceptionMapper
    public Object handleException(Exception ex, HttpHeaders headers) {
        if (expectsProtobuf(headers)) {
            TransportDispatchMetadata metadata = TransportDispatchMetadataHolder.get();
            String executionId = metadata == null ? null : metadata.executionId();
            LOG.errorf(ex, "Request failed (protobuf envelope), executionId=%s", executionId);
            Status status = ProtobufHttpStatusMapper.fromThrowable(ex, executionId, "rest");
            return Response.status(ProtobufHttpStatusMapper.toHttpStatus(status))
                .type(ProtobufHttpContentTypes.APPLICATION_X_PROTOBUF)
                .entity(status.toByteArray())
                .build();
        }
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
        Throwable rootCause = rootCause(ex);
        if (rootCause instanceof IllegalArgumentException) {
            LOG.warn("Invalid request", ex);
            return RestResponse.status(Response.Status.BAD_REQUEST, "Invalid request");
        }
        LOG.error("Unexpected error processing request", ex);
        return RestResponse.status(Response.Status.INTERNAL_SERVER_ERROR, "An unexpected error occurred");
    }

    /**
     * Determines whether the request represented by the provided headers indicates a Protobuf media type.
     *
     * @param headers HTTP headers from the request; may be null
     * @return true if the Accept or Content-Type header contains the Protobuf media type, false otherwise
     */
    private boolean expectsProtobuf(HttpHeaders headers) {
        if (headers == null) {
            return false;
        }
        String accept = headers.getHeaderString("Accept");
        String requestContentType = headers.getHeaderString("Content-Type");
        return containsProtobufMediaType(accept)
            || containsProtobufMediaType(requestContentType);
    }

    /**
     * Checks whether the given header value contains the Protobuf media type.
     *
     * @param value the header value to inspect (may be null)
     * @return `true` if `value` contains the Protobuf media type string, `false` otherwise
     */
    private boolean containsProtobufMediaType(String value) {
        return value != null && value.toLowerCase(java.util.Locale.ROOT)
            .contains(ProtobufHttpContentTypes.APPLICATION_X_PROTOBUF.toLowerCase(java.util.Locale.ROOT));
    }

    /**
     * Finds the deepest underlying cause in a throwable's cause chain.
     *
     * @param throwable the throwable to inspect; may be null
     * @return the deepest non-null cause found, or the original `throwable` (which may be null) if no deeper cause exists
     */
    private Throwable rootCause(Throwable throwable) {
        Throwable current = throwable;
        while (current != null && current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }
}
