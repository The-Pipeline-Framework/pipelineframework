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

import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.pipelineframework.service.ReactiveStreamingClientService;

class GrpcServiceClientStreamingAdapterTest {

    @Mock
    private ReactiveStreamingClientService<DomainIn, DomainOut> mockReactiveService;

    private GrpcServiceClientStreamingAdapter<GrpcIn, GrpcOut, DomainIn, DomainOut> adapter;

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

        adapter = new GrpcServiceClientStreamingAdapter<>() {
            @Override
            protected ReactiveStreamingClientService<DomainIn, DomainOut> getService() {
                return mockReactiveService;
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
                return false;
            }
        };
    }

    @Test
    void remoteProcess_SuccessPath() {
        // Given
        Multi<GrpcIn> grpcRequestStream = Multi.createFrom().items(new GrpcIn(), new GrpcIn());
        DomainOut domainOut = new DomainOut();

        Mockito.when(mockReactiveService.process(any(Multi.class)))
                .thenReturn(Uni.createFrom().item(domainOut));

        // When
        Uni<GrpcOut> resultUni = adapter.remoteProcess(grpcRequestStream);

        // Then
        UniAssertSubscriber<GrpcOut> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertNotNull(subscriber.getItem());
        Mockito.verify(mockReactiveService).process(any(Multi.class));
    }

    @Test
    void remoteProcess_FailurePath() {
        // Given
        Multi<GrpcIn> grpcRequestStream = Multi.createFrom().items(new GrpcIn(), new GrpcIn());
        RuntimeException testException = new RuntimeException("Processing failed");

        Mockito.when(mockReactiveService.process(any(Multi.class)))
                .thenReturn(Uni.createFrom().failure(testException));

        // When
        Uni<GrpcOut> resultUni = adapter.remoteProcess(grpcRequestStream);

        // Then
        UniAssertSubscriber<GrpcOut> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitFailure();

        Throwable failure = subscriber.getFailure();
        assertNotNull(failure);
        assertInstanceOf(StatusRuntimeException.class, failure);
        assertEquals("INTERNAL: Processing failed", failure.getMessage());
        assertEquals(testException.getMessage(), failure.getCause().getMessage());
    }

    @Test
    void remoteProcess_EmptyStream() {
        // Given
        Multi<GrpcIn> grpcRequestStream = Multi.createFrom().empty();
        DomainOut domainOut = new DomainOut();

        Mockito.when(mockReactiveService.process(any(Multi.class)))
                .thenReturn(Uni.createFrom().item(domainOut));

        // When
        Uni<GrpcOut> resultUni = adapter.remoteProcess(grpcRequestStream);

        // Then
        UniAssertSubscriber<GrpcOut> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertNotNull(subscriber.getItem());
        Mockito.verify(mockReactiveService).process(any(Multi.class));
    }

    @Test
    void remoteProcess_FromGrpcTransformation() {
        // Given
        GrpcIn grpcIn1 = new GrpcIn();
        GrpcIn grpcIn2 = new GrpcIn();
        Multi<GrpcIn> grpcRequestStream = Multi.createFrom().items(grpcIn1, grpcIn2);
        DomainOut domainOut = new DomainOut();

        // Create a mock to verify the transformation
        ReactiveStreamingClientService<DomainIn, DomainOut> verificationService = new ReactiveStreamingClientService<>() {
            @Override
            public Uni<DomainOut> process(Multi<DomainIn> inputStream) {
                // Collect the items to verify transformation
                return inputStream
                        .collect()
                        .asList()
                        .onItem()
                        .transform(
                                list -> {
                                    // Verify we received the expected number of items
                                    assertEquals(2, list.size());
                                    return domainOut;
                                });
            }
        };

        GrpcServiceClientStreamingAdapter<GrpcIn, GrpcOut, DomainIn, DomainOut> verificationAdapter = new GrpcServiceClientStreamingAdapter<>() {
            @Override
            protected ReactiveStreamingClientService<DomainIn, DomainOut> getService() {
                return verificationService;
            }

            @Override
            protected DomainIn fromGrpc(GrpcIn grpcIn) {
                // Transform to a domain object
                return new DomainIn();
            }

            @Override
            protected GrpcOut toGrpc(DomainOut domainOut) {
                return new GrpcOut();
            }

            @Override
            protected boolean isAutoPersistenceEnabled() {
                return false;
            }
        };

        // When
        Uni<GrpcOut> resultUni = verificationAdapter.remoteProcess(grpcRequestStream);

        // Then
        UniAssertSubscriber<GrpcOut> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertNotNull(subscriber.getItem());
    }

    @Test
    void remoteProcess_ToGrpcTransformation() {
        // Given
        Multi<GrpcIn> grpcRequestStream = Multi.createFrom().items(new GrpcIn());
        DomainOut domainOut = new DomainOut();

        Mockito.when(mockReactiveService.process(any(Multi.class)))
                .thenReturn(Uni.createFrom().item(domainOut));

        // Create an adapter that tracks the toGrpc transformation
        boolean[] toGrpcCalled = { false };
        GrpcServiceClientStreamingAdapter<GrpcIn, GrpcOut, DomainIn, DomainOut> trackingAdapter = new GrpcServiceClientStreamingAdapter<>() {
            @Override
            protected ReactiveStreamingClientService<DomainIn, DomainOut> getService() {
                return mockReactiveService;
            }

            @Override
            protected DomainIn fromGrpc(GrpcIn grpcIn) {
                return new DomainIn();
            }

            @Override
            protected GrpcOut toGrpc(DomainOut domainOut) {
                toGrpcCalled[0] = true;
                return new GrpcOut();
            }

            @Override
            protected boolean isAutoPersistenceEnabled() {
                return false;
            }
        };

        // When
        Uni<GrpcOut> resultUni = trackingAdapter.remoteProcess(grpcRequestStream);

        // Then
        UniAssertSubscriber<GrpcOut> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertTrue(toGrpcCalled[0], "toGrpc method should be called");
        assertNotNull(subscriber.getItem());
    }

    @Test
    void remoteProcess_ErrorInFromGrpcTransformation() {
        // Given
        GrpcIn grpcIn = new GrpcIn();
        Multi<GrpcIn> grpcRequestStream = Multi.createFrom().item(grpcIn);
        RuntimeException transformationException = new RuntimeException("Transformation failed");

        // Create an adapter that throws an exception in fromGrpc
        GrpcServiceClientStreamingAdapter<GrpcIn, GrpcOut, DomainIn, DomainOut> failingAdapter = new GrpcServiceClientStreamingAdapter<>() {
            @Override
            protected ReactiveStreamingClientService<DomainIn, DomainOut> getService() {
                // Return a service that will process the transformed stream
                // When the stream is processed, it will trigger the fromGrpc transformation
                return inputStream -> {
                    // Simulate what would happen in a real service - collect the
                    // stream
                    return inputStream
                            .collect()
                            .asList()
                            .onItem()
                            .transform(list -> new DomainOut());
                };
            }

            @Override
            protected DomainIn fromGrpc(GrpcIn grpcIn) {
                throw transformationException;
            }

            @Override
            protected GrpcOut toGrpc(DomainOut domainOut) {
                return new GrpcOut();
            }

            @Override
            protected boolean isAutoPersistenceEnabled() {
                return false;
            }
        };

        // When
        Uni<GrpcOut> resultUni = failingAdapter.remoteProcess(grpcRequestStream);

        // Then - we need to actually subscribe to trigger the stream processing
        UniAssertSubscriber<GrpcOut> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());

        // Wait for either completion or failure
        subscriber.awaitFailure();

        Throwable failure = subscriber.getFailure();
        assertNotNull(failure);
        assertInstanceOf(StatusRuntimeException.class, failure);
        assertTrue(failure.getMessage().contains("Transformation failed"));
    }

    @Test
    void remoteProcess_ErrorInToGrpcTransformation() {
        // Given
        Multi<GrpcIn> grpcRequestStream = Multi.createFrom().items(new GrpcIn());
        DomainOut domainOut = new DomainOut();
        RuntimeException transformationException = new RuntimeException("Output transformation failed");

        Mockito.when(mockReactiveService.process(any(Multi.class)))
                .thenReturn(Uni.createFrom().item(domainOut));

        // Create an adapter that throws an exception in toGrpc
        GrpcServiceClientStreamingAdapter<GrpcIn, GrpcOut, DomainIn, DomainOut> failingAdapter = new GrpcServiceClientStreamingAdapter<>() {
            @Override
            protected ReactiveStreamingClientService<DomainIn, DomainOut> getService() {
                return mockReactiveService;
            }

            @Override
            protected DomainIn fromGrpc(GrpcIn grpcIn) {
                return new DomainIn();
            }

            @Override
            protected GrpcOut toGrpc(DomainOut domainOut) {
                throw transformationException;
            }

            @Override
            protected boolean isAutoPersistenceEnabled() {
                return false;
            }
        };

        // When
        Uni<GrpcOut> resultUni = failingAdapter.remoteProcess(grpcRequestStream);

        // Then
        UniAssertSubscriber<GrpcOut> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitFailure();

        Throwable failure = subscriber.getFailure();
        assertNotNull(failure);
        assertInstanceOf(StatusRuntimeException.class, failure);
        assertTrue(failure.getMessage().contains("Output transformation failed"));
    }
}
