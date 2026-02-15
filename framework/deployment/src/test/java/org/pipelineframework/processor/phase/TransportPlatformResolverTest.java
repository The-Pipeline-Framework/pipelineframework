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

package org.pipelineframework.processor.phase;

import javax.annotation.processing.Messager;
import javax.tools.Diagnostic;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.config.PlatformMode;
import org.pipelineframework.processor.ir.TransportMode;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/** Unit tests for TransportPlatformResolver */
@ExtendWith(MockitoExtension.class)
class TransportPlatformResolverTest {

    private final TransportPlatformResolver resolver = new TransportPlatformResolver();

    @Mock
    private Messager messager;

    // --- Transport ---

    @Test
    void resolveTransport_grpc() {
        assertEquals(TransportMode.GRPC, resolver.resolveTransport("GRPC", messager));
    }

    @Test
    void resolveTransport_rest() {
        assertEquals(TransportMode.REST, resolver.resolveTransport("REST", messager));
    }

    @Test
    void resolveTransport_local() {
        assertEquals(TransportMode.LOCAL, resolver.resolveTransport("LOCAL", messager));
    }

    @Test
    void resolveTransport_null_defaultsToGrpc() {
        assertEquals(TransportMode.GRPC, resolver.resolveTransport(null, messager));
    }

    @Test
    void resolveTransport_blank_defaultsToGrpc() {
        assertEquals(TransportMode.GRPC, resolver.resolveTransport("  ", messager));
    }

    @Test
    void resolveTransport_unknown_warnsAndDefaultsToGrpc() {
        assertEquals(TransportMode.GRPC, resolver.resolveTransport("UNKNOWN", messager));
        verify(messager).printMessage(any(Diagnostic.Kind.class), contains("Unknown pipeline transport"));
    }

    @Test
    void resolveTransport_nullMessager_noException() {
        assertEquals(TransportMode.GRPC, resolver.resolveTransport("INVALID", null));
    }

    // --- Platform ---

    @Test
    void resolvePlatform_compute() {
        assertEquals(PlatformMode.COMPUTE, resolver.resolvePlatform("COMPUTE", messager));
    }

    @Test
    void resolvePlatform_function() {
        assertEquals(PlatformMode.FUNCTION, resolver.resolvePlatform("FUNCTION", messager));
    }

    @Test
    void resolvePlatform_null_defaultsToCompute() {
        assertEquals(PlatformMode.COMPUTE, resolver.resolvePlatform(null, messager));
    }

    @Test
    void resolvePlatform_blank_defaultsToCompute() {
        assertEquals(PlatformMode.COMPUTE, resolver.resolvePlatform("  ", messager));
    }

    @Test
    void resolvePlatform_unknown_warnsAndDefaultsToCompute() {
        assertEquals(PlatformMode.COMPUTE, resolver.resolvePlatform("UNKNOWN", messager));
        verify(messager).printMessage(any(Diagnostic.Kind.class), contains("Unknown pipeline platform"));
    }

    @Test
    void resolvePlatform_nullMessager_noException() {
        assertEquals(PlatformMode.COMPUTE, resolver.resolvePlatform("INVALID", null));
    }

    @Test
    void resolveTransport_validValues_noWarning() {
        resolver.resolveTransport("GRPC", messager);
        verify(messager, never()).printMessage(any(), any(String.class));
    }

    @Test
    void resolvePlatform_validValues_noWarning() {
        resolver.resolvePlatform("COMPUTE", messager);
        verify(messager, never()).printMessage(any(), any(String.class));
    }
}
