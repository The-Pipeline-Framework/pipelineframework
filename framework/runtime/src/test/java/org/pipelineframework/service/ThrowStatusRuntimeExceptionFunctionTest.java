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

package org.pipelineframework.service;

import static org.junit.jupiter.api.Assertions.*;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

class ThrowStatusRuntimeExceptionFunctionTest {

    @Test
    void apply_ShouldWrapThrowableInStatusRuntimeException() {
        // Given
        throwStatusRuntimeExceptionFunction function = new throwStatusRuntimeExceptionFunction();
        Throwable originalThrowable = new RuntimeException("Original error message");

        // When
        Throwable result = function.apply(originalThrowable);

        // Then
        assertNotNull(result);
        assertInstanceOf(StatusRuntimeException.class, result);

        StatusRuntimeException statusRuntimeException = (StatusRuntimeException) result;
        assertEquals(Status.INTERNAL.getCode(), statusRuntimeException.getStatus().getCode());
        assertEquals(
                originalThrowable.getMessage(),
                statusRuntimeException.getStatus().getDescription());
        assertSame(originalThrowable, statusRuntimeException.getStatus().getCause());

        Metadata metadata = statusRuntimeException.getTrailers();
        assertNotNull(metadata);
        assertEquals(
                originalThrowable.getMessage(),
                metadata.get(Metadata.Key.of("error-details", Metadata.ASCII_STRING_MARSHALLER)));
    }

    @Test
    void apply_ShouldSanitizeMultilineThrowableMessagesForGrpcHeaders() {
        // Given
        throwStatusRuntimeExceptionFunction function = new throwStatusRuntimeExceptionFunction();
        Throwable originalThrowable =
                new RuntimeException("CSV processing error:\nUnterminated quoted field\r\nBROKEN,USD");

        // When
        Throwable result = function.apply(originalThrowable);

        // Then
        assertInstanceOf(StatusRuntimeException.class, result);
        StatusRuntimeException statusRuntimeException = (StatusRuntimeException) result;
        String expected = "CSV processing error: Unterminated quoted field BROKEN,USD";

        assertEquals(expected, statusRuntimeException.getStatus().getDescription());
        assertSame(originalThrowable, statusRuntimeException.getStatus().getCause());

        Metadata metadata = statusRuntimeException.getTrailers();
        assertNotNull(metadata);
        assertEquals(
                expected,
                metadata.get(Metadata.Key.of("error-details", Metadata.ASCII_STRING_MARSHALLER)));
    }

    @Test
    void apply_ShouldUseClassSimpleNameWhenMessageIsNull() {
        // Given
        throwStatusRuntimeExceptionFunction function = new throwStatusRuntimeExceptionFunction();
        Throwable originalThrowable = new RuntimeException();

        // When
        Throwable result = function.apply(originalThrowable);

        // Then
        assertInstanceOf(StatusRuntimeException.class, result);
        StatusRuntimeException statusRuntimeException = (StatusRuntimeException) result;
        assertEquals("RuntimeException", statusRuntimeException.getStatus().getDescription());
        assertSame(originalThrowable, statusRuntimeException.getStatus().getCause());
    }

    @Test
    void apply_ShouldReturnUnknownErrorWhenThrowableIsNull() {
        // Given
        throwStatusRuntimeExceptionFunction function = new throwStatusRuntimeExceptionFunction();

        // When
        Throwable result = function.apply(null);

        // Then
        assertInstanceOf(StatusRuntimeException.class, result);
        StatusRuntimeException statusRuntimeException = (StatusRuntimeException) result;
        assertEquals("Unknown error", statusRuntimeException.getStatus().getDescription());
    }
}
