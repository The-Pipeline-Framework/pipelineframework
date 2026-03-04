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
import com.google.rpc.RequestInfo;
import com.google.rpc.Status;
import org.junit.jupiter.api.Test;
import org.pipelineframework.step.NonRetryableException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtobufHttpStatusMapperTest {

    @Test
    void mapsIllegalArgumentToInvalidArgumentStatus() {
        Status status = ProtobufHttpStatusMapper.fromThrowable(
            new IllegalArgumentException("bad input"), "exec-1", "step-1");

        assertEquals(Code.INVALID_ARGUMENT_VALUE, status.getCode());
        assertEquals(400, ProtobufHttpStatusMapper.toHttpStatus(status));
        assertTrue(status.getMessage().contains("bad input"));
    }

    @Test
    void mapsNonRetryableToFailedPreconditionStatus() {
        Status status = ProtobufHttpStatusMapper.fromThrowable(
            new NonRetryableException("do not retry"), "exec-2", "step-2");

        assertEquals(Code.FAILED_PRECONDITION_VALUE, status.getCode());
        assertEquals(412, ProtobufHttpStatusMapper.toHttpStatus(status));
        assertTrue(status.getMessage().contains("do not retry"));
        assertTrue(status.getMessage().contains("exec-2")
            || status.getDetailsList().stream().anyMatch(detail -> {
                if (detail.is(RequestInfo.class)) {
                    try {
                        return detail.unpack(RequestInfo.class).getRequestId().contains("exec-2");
                    } catch (com.google.protobuf.InvalidProtocolBufferException ignored) {
                        return false;
                    }
                }
                return false;
            }));
    }

    @Test
    void mapsUnknownFailureToInternalStatus() {
        Status status = ProtobufHttpStatusMapper.fromThrowable(
            new RuntimeException("boom"), "exec-3", "step-3");

        assertEquals(Code.INTERNAL_VALUE, status.getCode());
        assertEquals(500, ProtobufHttpStatusMapper.toHttpStatus(status));
        assertTrue(status.getMessage().contains("boom"));
        assertTrue(status.getMessage().contains("exec-3")
            || status.getDetailsList().stream().anyMatch(detail -> {
                if (detail.is(RequestInfo.class)) {
                    try {
                        return detail.unpack(RequestInfo.class).getRequestId().contains("exec-3");
                    } catch (com.google.protobuf.InvalidProtocolBufferException ignored) {
                        return false;
                    }
                }
                return false;
            }));
    }

    @Test
    void mapsNullThrowableToUnknownStatus() {
        Status status = ProtobufHttpStatusMapper.fromThrowable(null, "exec-null", "step-null");

        assertEquals(Code.UNKNOWN_VALUE, status.getCode());
        assertEquals(500, ProtobufHttpStatusMapper.toHttpStatus(status));
        assertTrue(status.getMessage().contains("No throwable provided"));
    }

    @Test
    void handlesMissingIdentifiersWithoutNpe() {
        Status withNulls = ProtobufHttpStatusMapper.fromThrowable(
            new IllegalArgumentException("bad"), null, null);
        Status withEmpty = ProtobufHttpStatusMapper.fromThrowable(
            new IllegalArgumentException("bad"), "", "");

        assertEquals(Code.INVALID_ARGUMENT_VALUE, withNulls.getCode());
        assertEquals(400, ProtobufHttpStatusMapper.toHttpStatus(withNulls));
        assertEquals(Code.INVALID_ARGUMENT_VALUE, withEmpty.getCode());
        assertEquals(400, ProtobufHttpStatusMapper.toHttpStatus(withEmpty));
    }

    @Test
    void mapsWrappedIllegalArgumentToInvalidArgumentStatus() {
        Status status = ProtobufHttpStatusMapper.fromThrowable(
            new RuntimeException("wrapper", new IllegalArgumentException("bad wrapped input")),
            "exec-wrapped",
            "step-wrapped");

        assertEquals(Code.INVALID_ARGUMENT_VALUE, status.getCode());
        assertEquals(400, ProtobufHttpStatusMapper.toHttpStatus(status));
        assertTrue(status.getMessage().contains("bad wrapped input"));
    }

    @Test
    void mapsAllStandardHttpStatusCodes() {
        assertEquals(200, ProtobufHttpStatusMapper.toHttpStatus(createStatus(Code.OK_VALUE)));
        assertEquals(499, ProtobufHttpStatusMapper.toHttpStatus(createStatus(Code.CANCELLED_VALUE)));
        assertEquals(400, ProtobufHttpStatusMapper.toHttpStatus(createStatus(Code.INVALID_ARGUMENT_VALUE)));
        assertEquals(404, ProtobufHttpStatusMapper.toHttpStatus(createStatus(Code.NOT_FOUND_VALUE)));
        assertEquals(409, ProtobufHttpStatusMapper.toHttpStatus(createStatus(Code.ALREADY_EXISTS_VALUE)));
        assertEquals(401, ProtobufHttpStatusMapper.toHttpStatus(createStatus(Code.UNAUTHENTICATED_VALUE)));
        assertEquals(403, ProtobufHttpStatusMapper.toHttpStatus(createStatus(Code.PERMISSION_DENIED_VALUE)));
        assertEquals(429, ProtobufHttpStatusMapper.toHttpStatus(createStatus(Code.RESOURCE_EXHAUSTED_VALUE)));
        assertEquals(412, ProtobufHttpStatusMapper.toHttpStatus(createStatus(Code.FAILED_PRECONDITION_VALUE)));
        assertEquals(409, ProtobufHttpStatusMapper.toHttpStatus(createStatus(Code.ABORTED_VALUE)));
        assertEquals(400, ProtobufHttpStatusMapper.toHttpStatus(createStatus(Code.OUT_OF_RANGE_VALUE)));
        assertEquals(501, ProtobufHttpStatusMapper.toHttpStatus(createStatus(Code.UNIMPLEMENTED_VALUE)));
        assertEquals(500, ProtobufHttpStatusMapper.toHttpStatus(createStatus(Code.INTERNAL_VALUE)));
        assertEquals(503, ProtobufHttpStatusMapper.toHttpStatus(createStatus(Code.UNAVAILABLE_VALUE)));
        assertEquals(504, ProtobufHttpStatusMapper.toHttpStatus(createStatus(Code.DEADLINE_EXCEEDED_VALUE)));
    }

    @Test
    void mapsNullStatusTo500() {
        assertEquals(500, ProtobufHttpStatusMapper.toHttpStatus(null));
    }

    @Test
    void mapsUnknownCodeTo500() {
        Status status = Status.newBuilder().setCode(999).setMessage("unknown code").build();
        assertEquals(500, ProtobufHttpStatusMapper.toHttpStatus(status));
    }

    @Test
    void preservesErrorDetailsInStatus() {
        Status status = ProtobufHttpStatusMapper.fromThrowable(
            new IllegalArgumentException("validation failed"), "exec-123", "validate-step");

        assertTrue(status.getDetailsCount() > 0, "Expected error details to be present");
    }

    @Test
    void handlesCircularCauseChainsGracefully() {
        RuntimeException ex1 = new RuntimeException("exception 1");
        RuntimeException ex2 = new RuntimeException("exception 2", ex1);
        try {
            java.lang.reflect.Field field = Throwable.class.getDeclaredField("cause");
            field.setAccessible(true);
            field.set(ex1, ex2);
        } catch (Exception ignored) {
            // If reflection fails, skip circular reference setup
        }

        Status status = ProtobufHttpStatusMapper.fromThrowable(ex2, "exec-circular", "step-circular");
        assertEquals(Code.INTERNAL_VALUE, status.getCode());
    }

    private Status createStatus(int code) {
        return Status.newBuilder().setCode(code).setMessage("test message").build();
    }
}