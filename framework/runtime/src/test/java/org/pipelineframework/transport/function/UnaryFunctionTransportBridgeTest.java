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

package org.pipelineframework.transport.function;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UnaryFunctionTransportBridgeTest {

    @Test
    void executesUnarySourceInvokeSinkFlow() {
        FunctionTransportContext context = FunctionTransportContext.of("req-1", "search-handler", "ingress");
        DefaultUnaryFunctionSourceAdapter<String> source = new DefaultUnaryFunctionSourceAdapter<>(
            "search.raw-document",
            "v1");
        LocalUnaryFunctionInvokeAdapter<String, Integer> invoke = new LocalUnaryFunctionInvokeAdapter<>(
            payload -> Uni.createFrom().item(payload.length()),
            "search.raw-document.length",
            "v1");
        DefaultUnaryFunctionSinkAdapter<Integer> sink = new DefaultUnaryFunctionSinkAdapter<>();

        Integer response = UnaryFunctionTransportBridge.invoke("hello", context, source, invoke, sink);

        assertEquals(5, response);
    }

    @Test
    void rejectsNonUnaryIngressShape() {
        FunctionTransportContext context = FunctionTransportContext.of("req-2", "search-handler", "ingress");
        FunctionSourceAdapter<String, String> source = (event, ctx) -> Multi.createFrom().items(
            TraceEnvelope.root("trace-a", "item-a", "search.raw-document", "v1", "idem-a", event),
            TraceEnvelope.root("trace-b", "item-b", "search.raw-document", "v1", "idem-b", event));
        LocalUnaryFunctionInvokeAdapter<String, Integer> invoke = new LocalUnaryFunctionInvokeAdapter<>(
            payload -> Uni.createFrom().item(payload.length()),
            "search.raw-document.length",
            "v1");
        DefaultUnaryFunctionSinkAdapter<Integer> sink = new DefaultUnaryFunctionSinkAdapter<>();

        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> UnaryFunctionTransportBridge.invoke("hello", context, source, invoke, sink));
        assertEquals("Function transport expected exactly one source item but received 2.", ex.getMessage());
    }

    @Test
    void rejectsEmptyIngressShape() {
        FunctionTransportContext context = FunctionTransportContext.of("req-3", "search-handler", "ingress");
        FunctionSourceAdapter<String, String> source = (event, ctx) -> Multi.createFrom().empty();
        LocalUnaryFunctionInvokeAdapter<String, Integer> invoke = new LocalUnaryFunctionInvokeAdapter<>(
            payload -> Uni.createFrom().item(payload.length()),
            "search.raw-document.length",
            "v1");
        DefaultUnaryFunctionSinkAdapter<Integer> sink = new DefaultUnaryFunctionSinkAdapter<>();

        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> UnaryFunctionTransportBridge.invoke("hello", context, source, invoke, sink));
        assertEquals("Function transport expected exactly one source item but received 0.", ex.getMessage());
    }
}
