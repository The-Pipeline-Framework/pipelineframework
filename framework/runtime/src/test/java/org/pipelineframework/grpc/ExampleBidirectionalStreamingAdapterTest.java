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

import io.smallrye.mutiny.Multi;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.pipelineframework.service.ReactiveBidirectionalStreamingService;

class ExampleBidirectionalStreamingAdapterTest {

    static class TestBidirectionalAdapter
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
    }

    @Test
    void testExampleBidirectionalAdapter() throws ExecutionException, InterruptedException {
        ExampleBidirectionalStreamingService service = new ExampleBidirectionalStreamingService();
        TestBidirectionalAdapter adapter = new TestBidirectionalAdapter(service);

        // Create input stream with 2 items
        Multi<String> input = Multi.createFrom().items("input1", "input2");

        // Process through adapter
        Multi<String> result = adapter.remoteProcess(input);

        // Collect results
        List<String> results = new ArrayList<>();
        CompletableFuture<Void> completion = new CompletableFuture<>();

        result.subscribe()
                .with(
                        item -> results.add(item),
                        failure -> completion.completeExceptionally(failure),
                        () -> completion.complete(null));

        // Wait for completion
        completion.get();

        // Each input generates 3 outputs, so we should have 6 total outputs
        assertEquals(6, results.size());

        // Verify the expected outputs
        assertTrue(results.contains("grpc_processed_domain_input1_part1"));
        assertTrue(results.contains("grpc_processed_domain_input1_part2"));
        assertTrue(results.contains("grpc_processed_domain_input1_part3"));
        assertTrue(results.contains("grpc_processed_domain_input2_part1"));
        assertTrue(results.contains("grpc_processed_domain_input2_part2"));
        assertTrue(results.contains("grpc_processed_domain_input2_part3"));
    }
}
