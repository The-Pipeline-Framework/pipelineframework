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

import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.runtime.LaunchMode;

import static java.text.MessageFormat.format;

/**
 * Configuration class for pipeline steps that manages different profiles and their settings.
 */
@ApplicationScoped
public final class PipelineConfig {

    private final java.util.Map<String, StepConfig> profiles = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicReference<String> activeProfile = new java.util.concurrent.atomic.AtomicReference<>();
    private volatile ParallelismPolicy parallelism = ParallelismPolicy.AUTO;
    private volatile int maxConcurrency = 128;

    /**
     * Initialises the PipelineConfig with a default profile and sets the active profile
     * from the Quarkus launch profile.
     *
     * <p>Creates a new StepConfig stored under the "default" profile key, reads the
     * current Quarkus launch profile key, falls back to "default" if the profile key
     * is null or blank, and activates the determined profile.</p>
     */
    public PipelineConfig() {
        // initialize with a default profile
        StepConfig defaultConfig = new StepConfig();
        profiles.put("default", defaultConfig);

        // sync with Quarkus profile
        String quarkusProfile = LaunchMode.current().getProfileKey();
        if (quarkusProfile == null || quarkusProfile.isBlank()) {
            quarkusProfile = "default";
        }
        activate(quarkusProfile);
    }

    /**
     * Which profile is currently active
     *
     * @return the name of the currently active profile
     */
    public String activeProfile() {
        return activeProfile.get();
    }

    /**
     * Switch active profile (must already exist or will be created)
     *
     * @param profileName the name of the profile to activate
     */
    public void activate(String profileName) {
        profiles.computeIfAbsent(profileName, ignored -> new StepConfig());
        activeProfile.set(profileName);
    }

    /**
     * Get the StepConfig for the current profile
     *
     * @return the StepConfig for the active profile
     */
    public StepConfig defaults() {
        return profiles.get(activeProfile());
    }

    /**
     * Add or update a profile explicitly
     *
     * @param name the name of the profile to add or update
     * @param config the StepConfig to associate with the profile
     * @return this PipelineConfig instance for method chaining
     */
    public PipelineConfig profile(String name, StepConfig config) {
        profiles.put(name, config);
        return this;
    }

    /**
     * Create a new StepConfig populated from the active profile's defaults.
     *
     * The returned config copies the active profile's settings for
     * retryLimit, retryWait, backpressureBufferCapacity, backpressureStrategy,
     * parallel, recoverOnFailure, maxBackoff and jitter.
     *
     * @return a new StepConfig initialised with the active profile's corresponding settings
     */
    public StepConfig newStepConfig() {
        StepConfig base = defaults();
        return new StepConfig()
                .retryLimit(base.retryLimit())
                .retryWait(base.retryWait())
                .backpressureBufferCapacity(base.backpressureBufferCapacity())
                .backpressureStrategy(base.backpressureStrategy())
                .recoverOnFailure(base.recoverOnFailure())
                .maxBackoff(base.maxBackoff())
                .jitter(base.jitter());
    }

    /**
     * Get the pipeline-level parallelism policy.
     *
     * @return the configured parallelism policy
     */
    public ParallelismPolicy parallelism() {
        return parallelism;
    }

    /**
     * Set the pipeline-level parallelism policy.
     *
     * @param policy the policy to use
     * @return this PipelineConfig instance for method chaining
     */
    public PipelineConfig parallelism(ParallelismPolicy policy) {
        this.parallelism = policy == null ? ParallelismPolicy.AUTO : policy;
        return this;
    }

    /**
     * Get the maximum concurrency for parallel execution.
     *
     * @return maximum in-flight items
     */
    public int maxConcurrency() {
        return maxConcurrency;
    }

    /**
     * Set the maximum concurrency for parallel execution.
     *
     * @param maxConcurrency maximum in-flight items
     * @return this PipelineConfig instance for method chaining
     */
    public PipelineConfig maxConcurrency(int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
        return this;
    }

    @Override
    public String toString() {
        return format("PipelineConfig'{'active={0}, profiles={1}'}'", activeProfile(), profiles.keySet());
    }
}
