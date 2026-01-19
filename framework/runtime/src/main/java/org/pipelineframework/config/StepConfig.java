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

package org.pipelineframework.config;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import jakarta.enterprise.context.Dependent;

import io.quarkus.arc.DefaultBean;

/**
 * Configuration class for pipeline steps that holds various runtime parameters
 * such as retry limits, concurrency settings, and debugging options.
 *
 * This class can be initialized with values from the new Quarkus configuration system.
 */
@Dependent
@DefaultBean
public class StepConfig {

    // Default values for configuration
    private static final int DEFAULT_RETRY_LIMIT = 3;
    private static final Duration DEFAULT_RETRY_WAIT = Duration.ofMillis(2000);
    private static final int DEFAULT_BACKPRESSURE_BUFFER_CAPACITY = 1024;
    private static final Duration DEFAULT_MAX_BACKOFF = Duration.ofSeconds(30);
    private static final String DEFAULT_BACKPRESSURE_STRATEGY = "BUFFER";

    // Mutable fields for runtime configuration (to maintain backward compatibility)
    private final AtomicInteger retryLimit = new AtomicInteger(DEFAULT_RETRY_LIMIT);
    private final AtomicReference<Duration> retryWait = new AtomicReference<>(DEFAULT_RETRY_WAIT);
    private final AtomicInteger backpressureBufferCapacity = new AtomicInteger(DEFAULT_BACKPRESSURE_BUFFER_CAPACITY);

    private volatile boolean recoverOnFailure = false;
    private volatile String backpressureStrategy = DEFAULT_BACKPRESSURE_STRATEGY;

    private final AtomicReference<Duration> maxBackoff = new AtomicReference<>(DEFAULT_MAX_BACKOFF);
    private volatile boolean jitter = false;

    /**
     * Creates a new StepConfig with default values.
     */
    public StepConfig() {}

    /**
     * Initialise a StepConfig from a PipelineStepConfig.StepConfig by copying its properties.
     *
     * If {@code config} is {@code null}, the instance retains its default values.
     *
     * @param config the Quarkus-backed step configuration whose values are copied; may be {@code null}
     */
    public StepConfig(org.pipelineframework.config.PipelineStepConfig.StepConfig config) {
        if (config != null) {
            this.retryLimit.set(config.retryLimit());
            this.retryWait.set(Duration.ofMillis(config.retryWaitMs()));
            this.recoverOnFailure = config.recoverOnFailure();
            this.maxBackoff.set(Duration.ofMillis(config.maxBackoff()));
            this.jitter = config.jitter();
            this.backpressureBufferCapacity.set(config.backpressureBufferCapacity());
            this.backpressureStrategy = config.backpressureStrategy();
        }
    }

    // --- getters ---
    /**
 * Number of retry attempts for a failed operation before giving up.
 *
 * @return the retry limit (number of attempts); default: 3
 */
    public int retryLimit() { return retryLimit.get(); }

    /**
     * Base delay between retry attempts
     * @return the retry wait duration (default: 2000ms)
     */
    public Duration retryWait() { return retryWait.get(); }

    /**
 * Current backpressure buffer capacity.
 *
 * @return the buffer capacity (number of elements)
 */
    public int backpressureBufferCapacity() { return backpressureBufferCapacity.get(); }

    /**
     * Backpressure strategy to use when buffering items ("BUFFER", "DROP")
     * @return the backpressure strategy (default: "BUFFER")
     */
    public String backpressureStrategy() { return backpressureStrategy; }

    /**
 * Indicates if failure recovery is enabled.
 *
 * @return `true` if failure recovery is enabled, `false` otherwise
 */
    public boolean recoverOnFailure() { return recoverOnFailure; }

    /**
 * Maximum duration allowed for exponential backoff.
 *
 * @return the maximum backoff duration, defaults to 30 seconds
 */
    public Duration maxBackoff() { return maxBackoff.get(); }

    /**
     * Whether to add jitter to backoff intervals
     * @return true if jitter is enabled, false otherwise
     */
    public boolean jitter() { return jitter; }

    // --- setters ---
    /**
         * Configure how many times a failed operation will be retried before no further retries are attempted.
         *
         * @param v the retry limit; must be greater than or equal to 0
         * @return   this StepConfig instance for method chaining
         * @throws IllegalArgumentException if {@code v} is less than 0
         */
    public StepConfig retryLimit(int v) {
        if (v < 0) {
            throw new IllegalArgumentException("retryLimit must be >= 0");
        }
        retryLimit.set(v);
        return this;
    }

    /**
     * Configure the base delay used between retry attempts.
     *
     * @param v the retry wait duration; must not be {@code null} and must be greater than zero
     * @return this StepConfig instance for method chaining
     * @throws NullPointerException if {@code v} is {@code null}
     * @throws IllegalArgumentException if {@code v} is zero or negative
     */
    public StepConfig retryWait(Duration v) {
        Objects.requireNonNull(v, "retryWait must not be null");
        if (v.isNegative() || v.isZero()) {
            throw new IllegalArgumentException("retryWait must be > 0");
        }
        retryWait.set(v);
        return this;
    }

    /**
     * Set the capacity of the backpressure buffer.
     *
     * @param v the capacity in number of items; must be greater than zero
     * @return this StepConfig instance for method chaining
     * @throws IllegalArgumentException if {@code v} is less than or equal to zero
     */
    public StepConfig backpressureBufferCapacity(int v) {
        if (v <= 0) {
            throw new IllegalArgumentException("backpressureBufferCapacity must be > 0");
        }
        backpressureBufferCapacity.set(v);
        return this;
    }

    /**
     * Sets whether to attempt recovery when a failure occurs
     * @param v true to enable failure recovery, false to disable
     * @return this StepConfig instance for method chaining
     */
    public StepConfig recoverOnFailure(boolean v) { recoverOnFailure = v; return this; }

    /**
     * Configure the backpressure strategy used when handling excess items.
     *
     * <p>Accepted values are "BUFFER" and "DROP"; comparison is case-insensitive and leading/trailing
     * whitespace is ignored. The value is normalised to upper case when stored.</p>
     *
     * @param v the strategy name (for example "BUFFER" or "DROP")
     * @return this StepConfig instance for method chaining
     * @throws NullPointerException if {@code v} is null
     * @throws IllegalArgumentException if {@code v} is not "BUFFER" or "DROP"
     */
    public StepConfig backpressureStrategy(String v) {
        Objects.requireNonNull(v, "backpressureStrategy must not be null");
        String norm = v.trim().toUpperCase();
        if (!norm.equals("BUFFER") && !norm.equals("DROP")) {
            throw new IllegalArgumentException("backpressureStrategy must be BUFFER or DROP");
        }
        backpressureStrategy = norm;
        return this;
    }

    /**
     * Configure the maximum backoff duration used for exponential backoff.
     *
     * @param v the maximum backoff Duration; must be greater than zero
     * @return this StepConfig instance for method chaining
     * @throws NullPointerException if {@code v} is null
     * @throws IllegalArgumentException if {@code v} is zero or negative
     */
    public StepConfig maxBackoff(Duration v) {
        Objects.requireNonNull(v, "maxBackoff must not be null");
        if (v.isNegative() || v.isZero()) {
            throw new IllegalArgumentException("maxBackoff must be > 0");
        }
        maxBackoff.set(v);
        return this;
    }

    /**
     * Sets whether to add jitter to backoff intervals
     * @param v true to enable jitter, false to disable
     * @return this StepConfig instance for method chaining
     */
    public StepConfig jitter(boolean v) { jitter = v; return this; }

    /**
     * Produces a single-line, human-readable summary of this step's configuration.
     *
     * @return a formatted String containing the current values of
     *         retryLimit, retryWait, recoverOnFailure, maxBackoff, jitter,
     *         backpressureBufferCapacity and backpressureStrategy
     */
    @Override
    public String toString() {
        return String.format("StepConfig{retryLimit=%d, retryWait=%s, recoverOnFailure=%b, maxBackoff=%s, jitter=%b, backpressureBufferCapacity=%d, backpressureStrategy=%s}",
                retryLimit(),
                retryWait(),
                recoverOnFailure,
                maxBackoff(),
                jitter,
                backpressureBufferCapacity(),
                backpressureStrategy());
    }
}
