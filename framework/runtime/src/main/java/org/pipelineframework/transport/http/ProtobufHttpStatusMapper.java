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
    /**
     * Prevents instantiation of this utility class.
     */
    private ProtobufHttpStatusMapper() {
    }

    /**
     * Map a Throwable to a protobuf Status envelope enriched with execution and resource details.
     *
     * Maps common exception types to corresponding protobuf Status codes and attaches a reason
     * in the Status details; if the throwable is null or its root cause is unrecognized, a
     * default UNKNOWN or INTERNAL Status is produced.
     *
     * @param throwable   the failure to map (may be null)
     * @param executionId identifier for the execution or request associated with the failure
     * @param resourceName name of the resource or step related to the failure
     * @return a protobuf Status representing the mapped failure with attached details (error reason,
     *         request id and resource info when provided)
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
     * Map a protobuf Status to the corresponding HTTP status code.
     *
     * @param status the protobuf Status to map; may be null
     * @return the HTTP status code that corresponds to the protobuf status code, or 500 if the status is null or its code is unrecognized
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
     * Builds a protobuf Status containing the given code and message and attaches
     * structured details (ErrorInfo plus optional RequestInfo and ResourceInfo).
     *
     * @param code the numeric protobuf status code to set on the Status
     * @param message the status message; an empty string is used if null
     * @param executionId optional execution identifier; when non-blank it is added as RequestInfo.requestId
     * @param resourceName optional resource name; when non-blank it is added as ResourceInfo.resourceName with type "tpf.step"
     * @param reason optional reason for the ErrorInfo; "UNKNOWN" is used if null
     * @return a Status populated with the provided code/message and packed detail protos:
     *         an ErrorInfo (domain "org.pipelineframework"), and optionally RequestInfo and ResourceInfo
     */
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

    /**
     * Finds the deepest causal Throwable in a throwable chain.
     *
     * Traverses the chain of causes and returns the deepest non-null cause; if the input is null,
     * no deeper cause exists, or a cyclic cause chain is detected, the original throwable (which may be null)
     * is returned.
     *
     * @param throwable the throwable whose root cause should be found
     * @return the deepest non-null cause in the causal chain, or the original throwable if none found
     */
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
