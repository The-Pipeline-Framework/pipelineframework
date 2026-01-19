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

import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pipelineframework.service.ReactiveStreamingService;

class GrpcServiceStreamingAdapterTest {

    @Mock
    private ReactiveStreamingService<DomainIn, DomainOut> mockReactiveService;

    private GrpcServiceStreamingAdapter<GrpcIn, GrpcOut, DomainIn, DomainOut> adapter;

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

        adapter = new GrpcServiceStreamingAdapter<>() {
            @Override
            protected ReactiveStreamingService<DomainIn, DomainOut> getService() {
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
        };
    }

    @Test
    void testRemoteProcessSuccessPath() {
        // Given
        GrpcIn grpcRequest = new GrpcIn();
        DomainOut domainOut1 = new DomainOut();
        DomainOut domainOut2 = new DomainOut();
        Multi<DomainOut> serviceResponse = Multi.createFrom().items(domainOut1, domainOut2);

        when(mockReactiveService.process(any(DomainIn.class))).thenReturn(serviceResponse);

        // When
        Multi<GrpcOut> result = adapter.remoteProcess(grpcRequest);

        // Then
        AssertSubscriber<GrpcOut> subscriber = AssertSubscriber.create(2);
        result.subscribe().withSubscriber(subscriber);
        subscriber.awaitItems(2);

        assertEquals(2, subscriber.getItems().size());
        assertNotNull(subscriber.getItems().get(0));
        assertNotNull(subscriber.getItems().get(1));

        verify(mockReactiveService).process(any(DomainIn.class));
        verifyNoMoreInteractions(mockReactiveService);
    }

    @Test
    void testRemoteProcessFailurePath() {
        // Given
        GrpcIn grpcRequest = new GrpcIn();
        RuntimeException testException = new RuntimeException("Processing failed");

        when(mockReactiveService.process(any(DomainIn.class)))
                .thenReturn(Multi.createFrom().failure(testException));

        // When
        Multi<GrpcOut> result = adapter.remoteProcess(grpcRequest);

        // Then
        AssertSubscriber<GrpcOut> subscriber = AssertSubscriber.create(1);
        result.subscribe().withSubscriber(subscriber);
        subscriber.awaitFailure();

        Throwable failure = subscriber.getFailure();
        assertNotNull(failure);
        assertInstanceOf(StatusRuntimeException.class, failure);
        assertTrue(failure.getMessage().contains("Processing failed"));

        verify(mockReactiveService).process(any(DomainIn.class));
    }

    @Test
    void testFromGrpcTransformation() {
        // Given
        GrpcIn grpcRequest = new GrpcIn();

        // Create a verification service to check the transformation
        ReactiveStreamingService<DomainIn, DomainOut> verificationService = domainIn -> {
            assertNotNull(domainIn);
            return Multi.createFrom().items(new DomainOut());
        };

        GrpcServiceStreamingAdapter<GrpcIn, GrpcOut, DomainIn, DomainOut> verificationAdapter = new GrpcServiceStreamingAdapter<>() {
            @Override
            protected ReactiveStreamingService<DomainIn, DomainOut> getService() {
                return verificationService;
            }

            @Override
            protected DomainIn fromGrpc(GrpcIn grpcIn) {
                assertNotNull(grpcIn);
                return new DomainIn();
            }

            @Override
            protected GrpcOut toGrpc(DomainOut domainOut) {
                return new GrpcOut();
            }
        };

        // When
        Multi<GrpcOut> result = verificationAdapter.remoteProcess(grpcRequest);

        // Then
        AssertSubscriber<GrpcOut> subscriber = AssertSubscriber.create(1);
        result.subscribe().withSubscriber(subscriber);
        subscriber.awaitItems(1);

        assertEquals(1, subscriber.getItems().size());
    }

    @Test
    void testToGrpcTransformation() {
        // Given
        GrpcIn grpcRequest = new GrpcIn();
        DomainOut domainOut = new DomainOut();

        when(mockReactiveService.process(any(DomainIn.class)))
                .thenReturn(Multi.createFrom().items(domainOut));

        // Create a verification adapter to check the transformation
        GrpcServiceStreamingAdapter<GrpcIn, GrpcOut, DomainIn, DomainOut> verificationAdapter = new GrpcServiceStreamingAdapter<>() {
            @Override
            protected ReactiveStreamingService<DomainIn, DomainOut> getService() {
                return mockReactiveService;
            }

            @Override
            protected DomainIn fromGrpc(GrpcIn grpcIn) {
                return new DomainIn();
            }

            @Override
            protected GrpcOut toGrpc(DomainOut domainOut) {
                assertNotNull(domainOut);
                return new GrpcOut();
            }
        };

        // When
        Multi<GrpcOut> result = verificationAdapter.remoteProcess(grpcRequest);

        // Then
        AssertSubscriber<GrpcOut> subscriber = AssertSubscriber.create(1);
        result.subscribe().withSubscriber(subscriber);
        subscriber.awaitItems(1);

        assertEquals(1, subscriber.getItems().size());
        verify(mockReactiveService).process(any(DomainIn.class));
    }

    @Test
    void testEmptyStream() {
        // Given
        GrpcIn grpcRequest = new GrpcIn();

        when(mockReactiveService.process(any(DomainIn.class)))
                .thenReturn(Multi.createFrom().empty());

        // When
        Multi<GrpcOut> result = adapter.remoteProcess(grpcRequest);

        // Then
        AssertSubscriber<GrpcOut> subscriber = AssertSubscriber.create(1);
        result.subscribe().withSubscriber(subscriber);
        subscriber.awaitCompletion();

        assertEquals(0, subscriber.getItems().size());
        verify(mockReactiveService).process(any(DomainIn.class));
    }
}
