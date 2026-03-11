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

package org.pipelineframework.transport.http;

import com.google.rpc.Code;
import com.google.rpc.ErrorInfo;
import com.google.rpc.RequestInfo;
import com.google.rpc.ResourceInfo;
import com.google.rpc.Status;
import com.google.protobuf.Any;
import io.grpc.StatusRuntimeException;
import jakarta.ws.rs.NotFoundException;
import org.pipelineframework.cache.CacheMissException;
import org.pipelineframework.cache.CachePolicyViolation;
import org.pipelineframework.step.NonRetryableException;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * Maps runtime failures to/from {@link Status} envelopes for Protobuf-over-HTTP transport.
 */
public final class ProtobufHttpStatusMapper {
    private ProtobufHttpStatusMapper() {
    }

    /**
     * Converts a throwable into a protobuf status envelope with contextual details.
     *
     * @param throwable failure to map
     * @param executionId execution identifier
     * @param resourceName resource/step name
     * @return protobuf status
     */
    public static Status fromThrowable(Throwable throwable, String executionId, String resourceName) {
        if (throwable == null) {
            return baseStatus(Code.UNKNOWN_VALUE, "UNKNOWN: No throwable provided", executionId, resourceName, "NONE");
        }
        Throwable root = rootCause(throwable);
        if (root instanceof IllegalArgumentException) {
            return baseStatus(Code.INVALID_ARGUMENT_VALUE, root.getMessage(), executionId, resourceName, "INVALID_ARGUMENT");
        }
        if (root instanceof CacheMissException || root instanceof CachePolicyViolation) {
            return baseStatus(Code.FAILED_PRECONDITION_VALUE, root.getMessage(), executionId, resourceName, "FAILED_PRECONDITION");
        }
        if (root instanceof NotFoundException) {
            return baseStatus(Code.NOT_FOUND_VALUE, root.getMessage(), executionId, resourceName, "NOT_FOUND");
        }
        if (root instanceof NonRetryableException) {
            return baseStatus(Code.FAILED_PRECONDITION_VALUE, root.getMessage(), executionId, resourceName, "NON_RETRYABLE");
        }
        if (root instanceof StatusRuntimeException sre) {
            io.grpc.Status grpcStatus = sre.getStatus();
            Code code = Code.forNumber(grpcStatus.getCode().value());
            if (code == null) {
                code = Code.INTERNAL;
            }
            return baseStatus(code.getNumber(),
                grpcStatus.getDescription() == null ? "gRPC invocation failed" : grpcStatus.getDescription(),
                executionId, resourceName, "GRPC_STATUS");
        }
        return baseStatus(Code.INTERNAL_VALUE,
            root.getMessage() == null ? "An unexpected error occurred" : root.getMessage(),
            executionId, resourceName, "INTERNAL");
    }

    /**
     * Maps protobuf status code to HTTP status code.
     *
     * @param status protobuf status
     * @return HTTP status
     */
    public static int toHttpStatus(Status status) {
        if (status == null) {
            return 500;
        }
        Code code = Code.forNumber(status.getCode());
        if (code == null) {
            return 500;
        }
        return switch (code) {
            case OK -> 200;
            case CANCELLED -> 499;
            case INVALID_ARGUMENT -> 400;
            case OUT_OF_RANGE -> 400;
            case FAILED_PRECONDITION -> 412;
            case NOT_FOUND -> 404;
            case ALREADY_EXISTS -> 409;
            case ABORTED -> 409;
            case UNAUTHENTICATED -> 401;
            case PERMISSION_DENIED -> 403;
            case RESOURCE_EXHAUSTED -> 429;
            case DEADLINE_EXCEEDED -> 504;
            case UNAVAILABLE -> 503;
            case UNIMPLEMENTED -> 501;
            default -> 500;
        };
    }

    /**
     * Returns whether the protobuf status should be treated as retryable by transport callers.
     *
     * @param status protobuf status envelope
     * @return true when the failure is transient/retryable
     */
    public static boolean isRetryable(Status status) {
        if (status == null) {
            return true;
        }
        Code code = Code.forNumber(status.getCode());
        if (code == null) {
            return true;
        }
        return switch (code) {
            case OK,
                INVALID_ARGUMENT,
                FAILED_PRECONDITION,
                NOT_FOUND,
                ALREADY_EXISTS,
                CANCELLED,
                UNAUTHENTICATED,
                PERMISSION_DENIED,
                OUT_OF_RANGE,
                UNIMPLEMENTED -> false;
            default -> true;
        };
    }

    private static Status baseStatus(
        int code,
        String message,
        String executionId,
        String resourceName,
        String reason
    ) {
        Status.Builder builder = Status.newBuilder()
            .setCode(code)
            .setMessage(message == null ? "" : message);

        ErrorInfo errorInfo = ErrorInfo.newBuilder()
            .setReason(reason == null ? "UNKNOWN" : reason)
            .setDomain("org.pipelineframework")
            .build();
        builder.addDetails(Any.pack(errorInfo));

        if (executionId != null && !executionId.isBlank()) {
            RequestInfo requestInfo = RequestInfo.newBuilder()
                .setRequestId(executionId.strip())
                .build();
            builder.addDetails(Any.pack(requestInfo));
        }
        if (resourceName != null && !resourceName.isBlank()) {
            ResourceInfo resourceInfo = ResourceInfo.newBuilder()
                .setResourceType("tpf.step")
                .setResourceName(resourceName.strip())
                .build();
            builder.addDetails(Any.pack(resourceInfo));
        }
        return builder.build();
    }

    private static Throwable rootCause(Throwable throwable) {
        Throwable current = throwable;
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        while (current != null
            && current.getCause() != null
            && current.getCause() != current
            && visited.add(current)) {
            current = current.getCause();
        }
        return current == null ? throwable : current;
    }
}
