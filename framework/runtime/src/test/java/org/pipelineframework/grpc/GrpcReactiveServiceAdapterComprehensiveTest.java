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

import io.grpc.StatusRuntimeException;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.service.throwStatusRuntimeExceptionFunction;

@QuarkusTest
class GrpcReactiveServiceAdapterComprehensiveTest {

    @Mock
    private ReactiveService<DomainIn, DomainOut> mockReactiveService;

    private GrpcReactiveServiceAdapter<GrpcIn, GrpcOut, DomainIn, DomainOut> unitTestAdapter;

    // Test-specific adapter that bypasses transaction for unit testing of core logic
    private static class UnitTestGrpcReactiveServiceAdapter
            extends GrpcReactiveServiceAdapter<GrpcIn, GrpcOut, DomainIn, DomainOut> {
        private final ReactiveService<DomainIn, DomainOut> testService;

        public UnitTestGrpcReactiveServiceAdapter(ReactiveService<DomainIn, DomainOut> service) {
            this.testService = service;
        }

        @Override
        protected ReactiveService<DomainIn, DomainOut> getService() {
            return testService;
        }

        @Override
        protected DomainIn fromGrpc(GrpcIn grpcIn) {
            return new DomainIn();
        }

        @Override
        protected GrpcOut toGrpc(DomainOut domainOut) {
            return new GrpcOut();
        }

        // Override to bypass Panache transaction context requirement for unit tests
        public Uni<GrpcOut> remoteProcess(GrpcIn grpcRequest) {
            DomainIn entity = fromGrpc(grpcRequest);

            Uni<DomainOut> processedResult = getService().process(entity);

            return processedResult
                    .onItem()
                    .transform(this::toGrpc)
                    .onFailure()
                    .transform(new throwStatusRuntimeExceptionFunction());
        }
    }

    private static class GrpcIn {
    }

    private static class GrpcOut {
    }

    private static class DomainIn {
    }

    private static class DomainOut {
    }

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        // Setup for unit tests (bypasses transaction)
        unitTestAdapter = new UnitTestGrpcReactiveServiceAdapter(mockReactiveService);

        // Setup for integration tests (uses real adapter but with mock service)
        // Override remoteProcess to bypass Panache transaction for testing purposes
    }

    // Unit Test Cases
    @Test
    void unitTest_remoteProcess_SuccessPath() {
        GrpcIn grpcRequest = new GrpcIn();
        DomainOut domainOut = new DomainOut();

        Mockito.when(mockReactiveService.process(ArgumentMatchers.any(DomainIn.class)))
                .thenReturn(Uni.createFrom().item(domainOut));

        Uni<GrpcOut> resultUni = unitTestAdapter.remoteProcess(grpcRequest);

        UniAssertSubscriber<GrpcOut> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertNotNull(subscriber.getItem());
        Mockito.verify(mockReactiveService).process(ArgumentMatchers.any(DomainIn.class));
    }

    @Test
    void unitTest_remoteProcess_FailurePath() {
        GrpcIn grpcRequest = new GrpcIn();
        RuntimeException testException = new RuntimeException("Processing failed");

        Mockito.when(mockReactiveService.process(ArgumentMatchers.any(DomainIn.class)))
                .thenReturn(Uni.createFrom().failure(testException));

        Uni<GrpcOut> resultUni = unitTestAdapter.remoteProcess(grpcRequest);

        UniAssertSubscriber<GrpcOut> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitFailure();

        Throwable failure = subscriber.getFailure();
        assertNotNull(failure);
        assertInstanceOf(StatusRuntimeException.class, failure);
        assertEquals("INTERNAL: Processing failed", failure.getMessage());
        assertEquals(testException.getMessage(), failure.getCause().getMessage());
    }
}
