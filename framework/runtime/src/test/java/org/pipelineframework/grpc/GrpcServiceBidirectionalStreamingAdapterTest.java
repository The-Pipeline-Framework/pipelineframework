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

package org.pipelineframework.grpc;

import static org.junit.jupiter.api.Assertions.*;

import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pipelineframework.service.ReactiveBidirectionalStreamingService;
import org.pipelineframework.service.throwStatusRuntimeExceptionFunction;

@QuarkusTest
class GrpcServiceBidirectionalStreamingAdapterTest {

    @Mock
    private ReactiveBidirectionalStreamingService<String, String> mockService;

    private TestBidirectionalAdapter adapter;

    // Test-specific adapter that bypasses Panache transaction for testing
    private static class TestBidirectionalAdapter
            extends GrpcServiceBidirectionalStreamingAdapter<String, String, String, String> {

        private final ReactiveBidirectionalStreamingService<String, String> service;

        public TestBidirectionalAdapter(
                ReactiveBidirectionalStreamingService<String, String> service) {
            this.service = service;
        }

        @Override
        protected ReactiveBidirectionalStreamingService<String, String> getService() {
            return service;
        }

        @Override
        protected String fromGrpc(String grpcIn) {
            return "domain_" + grpcIn;
        }

        @Override
        protected String toGrpc(String domainOut) {
            return "grpc_" + domainOut;
        }

        // Override to bypass Panache transaction context requirement for testing
        @Override
        public Multi<String> remoteProcess(Multi<String> grpcRequest) {
            Multi<String> inputDomainStream = grpcRequest.onItem().transform(this::fromGrpc);

            Multi<String> processed = getService().process(inputDomainStream);

            return processed
                    .onItem()
                    .transform(this::toGrpc)
                    .onFailure()
                    .transform(new throwStatusRuntimeExceptionFunction());
        }
    }

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        adapter = new TestBidirectionalAdapter(mockService);
    }

    @Test
    void testFromGrpcTransformation() {
        String grpcInput = "test";
        String domainInput = adapter.fromGrpc(grpcInput);

        assertEquals("domain_test", domainInput);
    }

    @Test
    void testToGrpcTransformation() {
        String domainOutput = "test";
        String grpcOutput = adapter.toGrpc(domainOutput);

        assertEquals("grpc_test", grpcOutput);
    }
}
