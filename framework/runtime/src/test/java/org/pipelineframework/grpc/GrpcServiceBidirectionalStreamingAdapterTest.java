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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pipelineframework.persistence.PersistenceManager;
import org.pipelineframework.service.ReactiveBidirectionalStreamingService;
import org.pipelineframework.service.throwStatusRuntimeExceptionFunction;

@QuarkusTest
class GrpcServiceBidirectionalStreamingAdapterTest {

    @Mock
    private ReactiveBidirectionalStreamingService<String, String> mockService;

    @Mock
    private PersistenceManager mockPersistenceManager;

    private TestBidirectionalAdapter adapter;

    // Test-specific adapter that bypasses Panache transaction for testing
    private static class TestBidirectionalAdapter
            extends GrpcServiceBidirectionalStreamingAdapter<String, String, String, String> {

        private final ReactiveBidirectionalStreamingService<String, String> service;
        private boolean autoPersist = true;

        public TestBidirectionalAdapter(
                ReactiveBidirectionalStreamingService<String, String> service) {
            this.service = service;
        }

        public void setAutoPersistence(boolean autoPersist) {
            this.autoPersist = autoPersist;
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

        @Override
        protected boolean isAutoPersistenceEnabled() {
            return autoPersist;
        }

        // Override to bypass Panache transaction context requirement for testing
        @Override
        public Multi<String> remoteProcess(Multi<String> grpcRequest) {
            Multi<String> inputDomainStream = grpcRequest.onItem().transform(this::fromGrpc);

            Multi<String> processedStream = getService().process(inputDomainStream);

            Multi<String> withAutoPersistence = isAutoPersistenceEnabled()
                    ? processedStream
                            .onItem()
                            .call(
                                    domainItem ->
                                    // If auto-persistence is enabled, persist each
                                    // item after processing
                                    persistenceManager.persist(domainItem))
                    : processedStream;

            return withAutoPersistence
                    .onItem()
                    .transform(this::toGrpc)
                    .onFailure()
                    .transform(new throwStatusRuntimeExceptionFunction());
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        adapter = new TestBidirectionalAdapter(mockService);

        // Inject the mock persistence manager using reflection
        java.lang.reflect.Field field = GrpcServiceBidirectionalStreamingAdapter.class.getDeclaredField(
                "persistenceManager");
        field.setAccessible(true);
        field.set(adapter, mockPersistenceManager);
    }

    @Test
    void testRemoteProcessWithoutAutoPersistence() {
        adapter.setAutoPersistence(false);

        // Prepare mock service to return a stream of results
        Multi<String> mockResult = Multi.createFrom().items("result1", "result2");
        when(mockService.process(any(Multi.class))).thenReturn(mockResult);

        // Mock persistence calls to return the same item
        when(mockPersistenceManager.persist(any(String.class)))
                .thenAnswer(
                        invocation -> io.smallrye.mutiny.Uni.createFrom()
                                .item(invocation.getArgument(0)));

        // Create input stream
        Multi<String> input = Multi.createFrom().items("input1", "input2");

        // Execute the test with bypassed transaction mechanism
        Multi<String> result = adapter.remoteProcess(input);

        // Collect results with blocking wait
        List<String> results = result.collect().asList().await().indefinitely();

        // Verify results
        assertEquals(2, results.size());
        assertTrue(results.contains("grpc_result1"));
        assertTrue(results.contains("grpc_result2"));

        // Verify persistence was not called
        verify(mockPersistenceManager, never()).persist(any());
    }

    @Test
    void testRemoteProcessWithAutoPersistence() {
        adapter.setAutoPersistence(true);

        // Prepare mock service to return a stream of results
        Multi<String> mockResult = Multi.createFrom().items("result1", "result2");
        when(mockService.process(any(Multi.class))).thenReturn(mockResult);

        // Mock persistence calls to return the same item
        when(mockPersistenceManager.persist(any(String.class)))
                .thenAnswer(
                        invocation -> {
                            String argument = invocation.getArgument(0);
                            return io.smallrye.mutiny.Uni.createFrom().item(argument);
                        });

        // Create input stream
        Multi<String> input = Multi.createFrom().items("input1", "input2");

        // Execute the test with bypassed transaction mechanism
        Multi<String> result = adapter.remoteProcess(input);

        // Collect results with blocking wait
        List<String> results = result.collect().asList().await().indefinitely();

        // Verify results
        assertEquals(2, results.size());
        assertTrue(results.contains("grpc_result1"));
        assertTrue(results.contains("grpc_result2"));

        // Verify persistence was called for each input after transformation
        verify(mockPersistenceManager, times(2)).persist(any(String.class));
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
