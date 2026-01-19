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

package org.pipelineframework.step;

import java.time.Duration;

import org.pipelineframework.config.StepConfig;

/**
 * Interface for pipeline steps that can be configured at runtime.
 */
public interface Configurable {
    /**
     * Get the effective configuration for this step.
     *
     * @return the step configuration
     */
    StepConfig effectiveConfig();

    /**
 * Retrieve the configured maximum number of retry attempts for the step.
 *
 * @return the maximum number of retry attempts allowed for the step
 */
    default int retryLimit() { return effectiveConfig().retryLimit(); }
    /**
 * Gets the configured wait duration between retry attempts.
 *
 * @return the duration to wait between retry attempts
 */
default Duration retryWait() { return effectiveConfig().retryWait(); }
    /**
 * Indicates whether the step should attempt recovery when a failure occurs.
 *
 * @return {@code true} if the step should attempt recovery after a failure, {@code false} otherwise.
 */
default boolean recoverOnFailure() { return effectiveConfig().recoverOnFailure(); }
    /**
 * Maximum backoff duration applied between retry attempts.
 *
 * @return the maximum backoff {@link Duration} used when applying retry backoff
 */
default Duration maxBackoff() { return effectiveConfig().maxBackoff(); }
    /**
 * Indicates whether jitter is enabled for retry backoff.
 *
 * @return `true` if jitter is enabled, `false` otherwise.
 */
default boolean jitter() { return effectiveConfig().jitter(); }
    /**
 * Provides the backpressure buffer capacity configured for this step.
 *
 * @return the maximum number of items the backpressure buffer can hold
 */
default int backpressureBufferCapacity() { return effectiveConfig().backpressureBufferCapacity(); }
    /**
 * Get the configured backpressure strategy for the step.
 *
 * @return the backpressure strategy name from the effective configuration
 */
default String backpressureStrategy() { return effectiveConfig().backpressureStrategy(); }
    /**
 * Determines whether a failure should be retried by this step.
 *
 * @param failure the failure to evaluate
 * @return {@code true} if the failure is retryable, {@code false} otherwise
 */
    default boolean shouldRetry(Throwable failure) {
        if (failure == null) {
            return false;
        }
        if (containsNonRetryable(failure)) {
            return false;
        }
        if (containsClientError(failure)) {
            return false;
        }
        return !containsNullPointer(failure);
    }

    private boolean containsNonRetryable(Throwable failure) {
        return containsThrowable(failure, NonRetryableException.class);
    }

    private boolean containsNullPointer(Throwable failure) {
        return containsThrowable(failure, NullPointerException.class);
    }

    private boolean containsClientError(Throwable failure) {
        return containsThrowableWithPredicate(
            failure,
            t -> t instanceof jakarta.ws.rs.WebApplicationException ex
                && ex.getResponse() != null
                && ex.getResponse().getStatus() >= 400
                && ex.getResponse().getStatus() < 500
        );
    }

    private boolean containsThrowable(Throwable failure, Class<? extends Throwable> target) {
        return containsThrowableWithPredicate(failure, target::isInstance);
    }

    private boolean containsThrowableWithPredicate(
        Throwable failure,
        java.util.function.Predicate<Throwable> predicate
    ) {
        java.util.ArrayDeque<Throwable> queue = new java.util.ArrayDeque<>();
        java.util.Set<Throwable> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        queue.add(failure);
        while (!queue.isEmpty()) {
            Throwable current = queue.removeFirst();
            if (!seen.add(current)) {
                continue;
            }
            if (predicate.test(current)) {
                return true;
            }
            Throwable cause = current.getCause();
            if (cause != null && cause != current) {
                queue.add(cause);
            }
            for (Throwable suppressed : current.getSuppressed()) {
                if (suppressed != null) {
                    queue.add(suppressed);
                }
            }
        }
        return false;
    }

    /**
     * Initialises the implementing object using the provided step configuration.
     * <p>
     * Implementations must apply values from the given {@code StepConfig} to configure the step before use.
     *
     * @param config the configuration to apply; serves as the effective configuration for this step
     */
    void initialiseWithConfig(StepConfig config);
}
