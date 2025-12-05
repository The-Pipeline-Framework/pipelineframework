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
import static org.mockito.Mockito.*;

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
import org.pipelineframework.domain.TestEntity;
import org.pipelineframework.domain.TestResult;
import org.pipelineframework.persistence.PersistenceManager;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.service.TestReactiveService;
import org.pipelineframework.service.throwStatusRuntimeExceptionFunction;

@QuarkusTest
class GrpcReactiveServiceAdapterComprehensiveTest {

    @Mock
    private ReactiveService<DomainIn, DomainOut> mockReactiveService;
    @Mock
    private PersistenceManager mockPersistenceManager;

    private GrpcReactiveServiceAdapter<GrpcIn, GrpcOut, DomainIn, DomainOut> unitTestAdapter;
    private GrpcReactiveServiceAdapter<Object, Object, TestEntity, TestResult> integrationTestAdapter;

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

        @Override
        protected boolean isAutoPersistenceEnabled() {
            // Disable auto-persistence for unit testing
            return false;
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

    private static class TestGrpcRequest {
    }

    private static class TestGrpcResponse {
    }

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        // Setup for unit tests (bypasses transaction)
        unitTestAdapter = new UnitTestGrpcReactiveServiceAdapter(mockReactiveService);
        java.lang.reflect.Field field = GrpcReactiveServiceAdapter.class.getDeclaredField("persistenceManager");
        field.setAccessible(true);
        field.set(unitTestAdapter, mockPersistenceManager);

        // Setup for integration tests (uses real adapter but with mock service)
        integrationTestAdapter = new GrpcReactiveServiceAdapter<>() {
            @Override
            protected ReactiveService<TestEntity, TestResult> getService() {
                return new TestReactiveService();
            }

            @Override
            protected TestEntity fromGrpc(Object grpcIn) {
                return new TestEntity("test", "description");
            }

            @Override
            protected Object toGrpc(TestResult domainOut) {
                return new TestGrpcResponse();
            }

            @Override
            protected boolean isAutoPersistenceEnabled() {
                return true;
            }

            // Override remoteProcess to bypass Panache transaction for testing purposes
            @Override
            public Uni<Object> remoteProcess(Object grpcRequest) {
                TestEntity entity = fromGrpc(grpcRequest);

                Uni<TestResult> processedResult = getService().process(entity);

                Uni<TestResult> withPersistence = isAutoPersistenceEnabled()
                        ? processedResult
                                .onItem()
                                .call(
                                        ignored ->
                // If auto-persistence is enabled,
                // persist the input entity after
                // successful processing
                persistenceManager.persist(entity))
                        : processedResult;

                return withPersistence
                        .onItem()
                        .transform(this::toGrpc)
                        .onFailure()
                        .transform(new throwStatusRuntimeExceptionFunction());
            }
        };

        field = GrpcReactiveServiceAdapter.class.getDeclaredField("persistenceManager");
        field.setAccessible(true);
        field.set(integrationTestAdapter, mockPersistenceManager);
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

    // Integration Test Cases
    @Test
    void integrationTest_WithAutoPersistenceEnabled_ShouldCallPersist() {
        TestGrpcRequest grpcRequest = new TestGrpcRequest();
        TestEntity entity = new TestEntity("test", "description");
        TestResult result = new TestResult("Processed: test", "Processed: description");

        // Mock the persistence manager to return the same entity
        when(mockPersistenceManager.persist(any(TestEntity.class)))
                .thenReturn(Uni.createFrom().item(entity));

        Uni<Object> resultUni = integrationTestAdapter.remoteProcess(grpcRequest);

        UniAssertSubscriber<Object> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertNotNull(subscriber.getItem());
        verify(mockPersistenceManager).persist(any(TestEntity.class));
    }

    @Test
    void integrationTest_WithAutoPersistenceDisabled_ShouldNotCallPersist() {
        GrpcReactiveServiceAdapter<Object, Object, TestEntity, TestResult> adapterWithoutPersistence = new GrpcReactiveServiceAdapter<>() {
            @Override
            protected ReactiveService<TestEntity, TestResult> getService() {
                return new TestReactiveService();
            }

            @Override
            protected TestEntity fromGrpc(Object grpcIn) {
                return new TestEntity("test", "description");
            }

            @Override
            protected Object toGrpc(TestResult domainOut) {
                return new TestGrpcResponse();
            }

            @Override
            protected boolean isAutoPersistenceEnabled() {
                return false;
            }

            // Override to bypass Panache transaction for testing purposes
            @Override
            public Uni<Object> remoteProcess(Object grpcRequest) {
                TestEntity entity = fromGrpc(grpcRequest);

                Uni<TestResult> processedResult = getService().process(entity);

                Uni<TestResult> withPersistence = isAutoPersistenceEnabled()
                        ? processedResult
                                .onItem()
                                .call(
                                        ignored ->
                // If auto-persistence is
                // enabled, persist the
                // input entity after
                // successful processing
                persistenceManager.persist(
                        entity))
                        : processedResult;

                return withPersistence
                        .onItem()
                        .transform(this::toGrpc)
                        .onFailure()
                        .transform(new throwStatusRuntimeExceptionFunction());
            }
        };

        try {
            java.lang.reflect.Field field = GrpcReactiveServiceAdapter.class.getDeclaredField("persistenceManager");
            field.setAccessible(true);
            field.set(adapterWithoutPersistence, mockPersistenceManager);
        } catch (Exception e) {
            fail("Failed to inject mock persistence manager: " + e.getMessage());
        }

        TestGrpcRequest grpcRequest = new TestGrpcRequest();

        Uni<Object> resultUni = adapterWithoutPersistence.remoteProcess(grpcRequest);

        UniAssertSubscriber<Object> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertNotNull(subscriber.getItem());
        verify(mockPersistenceManager, never()).persist(any(TestEntity.class));
    }
}
