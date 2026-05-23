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
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitSuspendedException;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.step.StepManyToMany;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class StepManyToManyTest {

    static class TestStep implements StepManyToMany<Object, Object> {
        @Override
        public Multi<Object> applyTransform(Multi<Object> input) {
            return input.onItem().transform(item -> "Streamed: " + item);
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

    static class FailingRecoverStep implements StepManyToMany<Object, Object> {
        private final AtomicBoolean rejectCalled = new AtomicBoolean(false);

        @Override
        public Multi<Object> applyTransform(Multi<Object> input) {
            return Multi.createFrom().failure(new RuntimeException("boom"));
        }

        @Override
        public io.smallrye.mutiny.Uni<Object> rejectStream(
                List<Object> sampleItems,
                long totalItemCount,
                Throwable cause,
                Integer retriesObserved,
                Integer retryLimit) {
            rejectCalled.set(true);
            return io.smallrye.mutiny.Uni.createFrom().nullItem();
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

    @Test
    void testApplyStreamingMethod() {
        // Given
        TestStep step = new TestStep();
        Multi<Object> input = Multi.createFrom().items("item1", "item2");

        // When
        Multi<Object> result = step.apply(input);

        // Then
        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(2));
        subscriber.awaitItems(2, Duration.ofSeconds(5));
        subscriber.assertItems("Streamed: item1", "Streamed: item2");
    }

    @Test
    void testApplyMethod() {
        // Given
        TestStep step = new TestStep();
        Multi<Object> input = Multi.createFrom().items("item1", "item2");

        // When
        Multi<Object> result = (Multi<Object>) step.apply(input);

        // Then
        AssertSubscriber<Object> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(2));
        subscriber.awaitItems(2, Duration.ofSeconds(5));
        subscriber.assertItems("Streamed: item1", "Streamed: item2");
    }

    @Test
    void awaitSuspensionBypassesRejectSink() {
        AtomicInteger applyCalls = new AtomicInteger();
        FailingRecoverStep step = new FailingRecoverStep() {
            @Override
            public Multi<Object> applyTransform(Multi<Object> input) {
                applyCalls.incrementAndGet();
                return Multi.createFrom().failure(new AwaitSuspendedException("tenant", "execution", "interaction", 1));
            }
        };

        AssertSubscriber<Object> subscriber =
                step.apply(Multi.createFrom().items("item1", "item2"))
                        .subscribe().withSubscriber(AssertSubscriber.create());

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
            public Multi<Object> applyTransform(Multi<Object> input) {
                applyCalls.incrementAndGet();
                return Multi.createFrom().failure(new CancellationException("HTTP server call cancelled"));
            }

            @Override
            public StepConfig effectiveConfig() {
                return new StepConfig().recoverOnFailure(false).retryLimit(1);
            }
        };

        AssertSubscriber<Object> subscriber =
                step.apply(Multi.createFrom().items("item1", "item2"))
                        .subscribe().withSubscriber(AssertSubscriber.create());

        Throwable failure = subscriber.awaitFailure(Duration.ofSeconds(5)).getFailure();

        assertInstanceOf(CancellationException.class, failure);
        assertEquals(1, applyCalls.get());
        assertFalse(step.rejectCalled());
    }
}
