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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.config.PlatformMode;
import org.pipelineframework.processor.PipelineCompilerDiagnostics;
import org.pipelineframework.processor.ir.PipelineTransport;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/** Unit tests for TransportPlatformResolver */
@ExtendWith(MockitoExtension.class)
class TransportPlatformResolverTest {

    private final TransportPlatformResolver resolver = new TransportPlatformResolver();

    @Mock
    private PipelineCompilerDiagnostics diagnostics;

    // --- Transport ---

    @Test
    void resolveTransport_grpc() {
        assertEquals(PipelineTransport.GRPC, resolver.resolveTransport("GRPC", diagnostics));
    }

    @Test
    void resolveTransport_rest() {
        assertEquals(PipelineTransport.REST, resolver.resolveTransport("REST", diagnostics));
    }

    @Test
    void resolveTransport_local() {
        assertEquals(PipelineTransport.LOCAL, resolver.resolveTransport("LOCAL", diagnostics));
    }

    @Test
    void resolveTransport_null_defaultsToGrpc() {
        assertEquals(PipelineTransport.GRPC, resolver.resolveTransport(null, diagnostics));
    }

    @Test
    void resolveTransport_blank_defaultsToGrpc() {
        assertEquals(PipelineTransport.GRPC, resolver.resolveTransport("  ", diagnostics));
    }

    @Test
    void resolveTransport_unknown_warnsAndDefaultsToGrpc() {
        assertEquals(PipelineTransport.GRPC, resolver.resolveTransport("UNKNOWN", diagnostics));
        verify(diagnostics).warning(contains("Unknown pipeline transport"));
    }

    @Test
    void resolveTransport_nullDiagnostics_noException() {
        assertEquals(PipelineTransport.GRPC,
            resolver.resolveTransport("INVALID", null));
    }

    // --- Platform ---

    @Test
    void resolvePlatform_compute() {
        assertEquals(PlatformMode.COMPUTE, resolver.resolvePlatform("COMPUTE", diagnostics));
    }

    @Test
    void resolvePlatform_function() {
        assertEquals(PlatformMode.FUNCTION, resolver.resolvePlatform("FUNCTION", diagnostics));
    }

    @Test
    void resolvePlatform_null_defaultsToCompute() {
        assertEquals(PlatformMode.COMPUTE, resolver.resolvePlatform(null, diagnostics));
    }

    @Test
    void resolvePlatform_blank_defaultsToCompute() {
        assertEquals(PlatformMode.COMPUTE, resolver.resolvePlatform("  ", diagnostics));
    }

    @Test
    void resolvePlatform_unknown_warnsAndDefaultsToCompute() {
        assertEquals(PlatformMode.COMPUTE, resolver.resolvePlatform("UNKNOWN", diagnostics));
        verify(diagnostics).warning(contains("Unknown pipeline platform"));
    }

    @Test
    void resolvePlatform_nullDiagnostics_noException() {
        assertEquals(PlatformMode.COMPUTE,
            resolver.resolvePlatform("INVALID", null));
    }

    @Test
    void resolveTransport_caseInsensitive() {
        assertEquals(PipelineTransport.GRPC, resolver.resolveTransport("grpc", diagnostics));
        assertEquals(PipelineTransport.REST, resolver.resolveTransport("rest", diagnostics));
        assertEquals(PipelineTransport.LOCAL, resolver.resolveTransport("local", diagnostics));
    }

    @Test
    void resolvePlatform_caseInsensitive() {
        assertEquals(PlatformMode.COMPUTE, resolver.resolvePlatform("compute", diagnostics));
        assertEquals(PlatformMode.FUNCTION, resolver.resolvePlatform("function", diagnostics));
    }

    @Test
    void resolveTransport_validValues_noWarning() {
        resolver.resolveTransport("GRPC", diagnostics);
        verify(diagnostics, never()).report(any(), any(String.class));
    }

    @Test
    void resolvePlatform_validValues_noWarning() {
        resolver.resolvePlatform("COMPUTE", diagnostics);
        verify(diagnostics, never()).report(any(), any(String.class));
    }
}
