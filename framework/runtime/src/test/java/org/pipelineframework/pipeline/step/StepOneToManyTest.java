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

package org.pipelineframework.pipeline.step;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitSuspendedException;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.invocation.TransportBoundaryDescriptor;
import org.pipelineframework.invocation.TransportBoundaryInvocation;
import org.pipelineframework.step.StepOneToMany;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class StepOneToManyTest {

    static class TestStep implements StepOneToMany<String, String> {
        @Override
        public Multi<String> applyOneToMany(String input) {
            return Multi.createFrom().items(input + "-1", input + "-2", input + "-3");
        }

        @Override
        public StepConfig effectiveConfig() {
            return new StepConfig();
        }

        @Override
        public void initialiseWithConfig(org.pipelineframework.config.StepConfig config) {
            // Use the config provided
        }
    }

    static class FailingRecoverStep implements StepOneToMany<String, String> {
        private final AtomicBoolean rejectCalled = new AtomicBoolean(false);

        @Override
        public Multi<String> applyOneToMany(String input) {
            return Multi.createFrom().failure(new RuntimeException("boom"));
        }

        @Override
        public Uni<String> rejectStream(
                List<String> sampleItems,
                long totalItemCount,
                Throwable cause,
                Integer retriesObserved,
                Integer retryLimit) {
            rejectCalled.set(true);
            return Uni.createFrom().nullItem();
        }

        @Override
        public StepConfig effectiveConfig() {
            return new StepConfig().recoverOnFailure(true).retryLimit(1);
        }

        @Override
        public void initialiseWithConfig(org.pipelineframework.config.StepConfig config) {
            // no-op
        }

        boolean rejectCalled() {
            return rejectCalled.get();
        }
    }

    static class TransportFailingStep implements StepOneToMany<String, String>, TransportBoundaryInvocation {
        private final AtomicInteger applyCalls = new AtomicInteger();

        @Override
        public Multi<String> applyOneToMany(String input) {
            applyCalls.incrementAndGet();
            return Multi.createFrom().failure(new IllegalStateException("remote stream failed"));
        }

        @Override
        public StepConfig effectiveConfig() {
            return new StepConfig().retryLimit(3);
        }

        @Override
        public void initialiseWithConfig(StepConfig config) {
            // no-op
        }

        @Override
        public TransportBoundaryDescriptor transportBoundary() {
            return new TransportBoundaryDescriptor("grpc", "Input.remoteProcess");
        }

        int applyCalls() {
            return applyCalls.get();
        }
    }

    @Test
    void testApplyMultiMethod() {
        // Given
        TestStep step = new TestStep();

        // When
        Multi<String> result = step.applyOneToMany("test");

        // Then
        AssertSubscriber<String> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(3));
        subscriber.awaitItems(3, Duration.ofSeconds(5));
        subscriber.assertItems("test-1", "test-2", "test-3");
    }

    @Test
    void testApplyMethod() {
        // Given
        TestStep step = new TestStep();
        Multi<String> input = Multi.createFrom().items("item1", "item2");

        // When
        Multi<String> result = input.onItem()
                .transformToMulti(item -> step.apply(Uni.createFrom().item(item)))
                .concatenate();

        // Then
        AssertSubscriber<String> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(6));
        subscriber.awaitItems(6, Duration.ofSeconds(5));
        subscriber.assertItems("item1-1", "item1-2", "item1-3", "item2-1", "item2-2", "item2-3");
    }

    @Test
    void awaitSuspensionBypassesRejectSink() {
        AtomicInteger applyCalls = new AtomicInteger();
        FailingRecoverStep step = new FailingRecoverStep() {
            @Override
            public Multi<String> applyOneToMany(String input) {
                applyCalls.incrementAndGet();
                return Multi.createFrom().failure(new AwaitSuspendedException("tenant", "execution", "interaction", 1));
            }
        };

        AssertSubscriber<String> subscriber =
                step.apply(Uni.createFrom().item("x")).subscribe().withSubscriber(AssertSubscriber.create());

        Throwable failure = subscriber.awaitFailure(Duration.ofSeconds(5)).getFailure();

        assertInstanceOf(AwaitSuspendedException.class, failure);
        assertEquals(1, applyCalls.get());
        assertFalse(step.rejectCalled());
    }

    @Test
    void cancellationDoesNotRetry() {
        AtomicInteger applyCalls = new AtomicInteger();
        FailingRecoverStep step = new FailingRecoverStep() {
            @Override
            public Multi<String> applyOneToMany(String input) {
                applyCalls.incrementAndGet();
                return Multi.createFrom().failure(new CancellationException("HTTP server call cancelled"));
            }

            @Override
            public StepConfig effectiveConfig() {
                return new StepConfig().recoverOnFailure(false).retryLimit(1);
            }
        };

        AssertSubscriber<String> subscriber =
                step.apply(Uni.createFrom().item("x")).subscribe().withSubscriber(AssertSubscriber.create());

        Throwable failure = subscriber.awaitFailure(Duration.ofSeconds(5)).getFailure();

        assertInstanceOf(CancellationException.class, failure);
        assertEquals(1, applyCalls.get());
        assertFalse(step.rejectCalled());
    }

    @Test
    void transportBoundaryStreamDoesNotApplyGenericStepRetry() {
        TransportFailingStep step = new TransportFailingStep();

        AssertSubscriber<String> subscriber =
                step.apply(Uni.createFrom().item("x")).subscribe().withSubscriber(AssertSubscriber.create());

        Throwable failure = subscriber.awaitFailure(Duration.ofSeconds(5)).getFailure();

        assertInstanceOf(IllegalStateException.class, failure);
        assertEquals(1, step.applyCalls());
    }

}
