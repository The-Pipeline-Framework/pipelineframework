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
}
