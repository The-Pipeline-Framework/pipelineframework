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
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.quarkus.arc.Unremovable;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

/**
 * Configuration mapping for pipeline steps, supporting both global defaults
 * and per-step overrides using Quarkus configuration patterns.
 * <p>
 * To configure global defaults: pipeline.defaults.property=value
 * To configure specific steps: pipeline.step."fully.qualified.StepClass".property=value
 */
@ConfigMapping(prefix = "pipeline")
@Unremovable
public interface PipelineStepConfig {

    /**
     * Default configuration applied to every pipeline step.
     * <p>
     * When used as global defaults, properties are applied to every step unless a step provides overrides.
     *
     * @return the StepConfig containing default values for pipeline steps
     */
    StepConfig defaults();

    /**
     * Pipeline-level parallelism policy for per-item steps.
     *
     * @return the parallelism policy
     */
    @WithDefault("AUTO")
    ParallelismPolicy parallelism();

    /**
     * Maximum number of concurrent in-flight items when parallel execution is enabled.
     *
     * @return maximum concurrency
     */
    @WithDefault("128")
    Integer maxConcurrency();

    /**
     * Health check configuration for pipeline startup.
     *
     * @return health check configuration
     */
    HealthConfig health();

    /**
     * Cache configuration for pipeline cache providers.
     *
     * @return cache configuration
     */
    CacheConfig cache();
    
    /**
     * Per-step configuration overrides keyed by each step's fully qualified class name.
     *
     * <p>Configured under the prefix <code>pipeline.step."fully.qualified.class.name".property=value</code>.
     *
     * @return a map from fully qualified step class name to the corresponding {@link StepConfig} override
     */
    @WithName("step")
    Map<String, StepConfig> step();

    /**
     * Module overrides used for orchestrator client wiring.
     *
     * @return module overrides keyed by module name
     */
    @WithName("module")
    Map<String, ModuleConfig> module();

    /**
     * Client wiring defaults for generated orchestrator clients.
     *
     * @return client wiring defaults
     */
    ClientConfig client();

    /**
     * Configuration for individual pipeline steps, allowing per-step override of global defaults.
     */
    interface StepConfig {
        /**
        /**
         * Maximum number of retry attempts for failed operations.
         * @return maximum retry attempts
         */
        @WithDefault("3")
        Integer retryLimit();

        /**
         * Base delay between retry attempts, in milliseconds.
         *
         * @return the base delay between retries in milliseconds
         */
        @WithDefault("2000")
        Long retryWaitMs();

        /**
         * Whether the step will attempt recovery after a failure.
         *
         * @return `true` if recovery is enabled, `false` otherwise
         */
        @WithDefault("false")
        Boolean recoverOnFailure();

        /**
         * Limit for backoff delays applied to retry attempts.
         *
         * @return the maximum backoff time in milliseconds
         */
        @WithDefault("30000")
        Long maxBackoff();

        /**
         * Whether jitter is added to retry delays.
         *
         * @return true to add jitter to retry delays, false otherwise.
         */
        @WithDefault("false")
        Boolean jitter();

        /**
         * Configures the capacity of the backpressure buffer for this pipeline step.
         *
         * @return the buffer capacity in number of items; default is 1024
         */
        @WithDefault("1024")
        Integer backpressureBufferCapacity();

        /**
         * Selects the backpressure strategy applied when buffering items.
         *
         * <p>Accepted values: "BUFFER" to buffer incoming items, "DROP" to discard items when capacity is reached.</p>
         *
         * @return the backpressure strategy; "BUFFER" by default
         */
        @WithDefault("BUFFER")
        String backpressureStrategy();
    }

    /**
     * Module configuration used for orchestrator client wiring.
     */
    interface ModuleConfig {
        /**
         * Host for all services in the module.
         *
         * @return host name
         */
        Optional<String> host();

        /**
         * Port for all services in the module.
         *
         * @return port
         */
        Optional<Integer> port();

        /**
         * Client names assigned to the module.
         *
         * @return client names
         */
        Optional<List<String>> steps();

        /**
         * Aspect names assigned to the module.
         *
         * @return aspect names
         */
        Optional<List<String>> aspects();
    }

    /**
     * Client wiring defaults for generated orchestrator clients.
     */
    interface ClientConfig {
        /**
         * Base port used when assigning per-module offsets.
         *
         * @return base port
         */
        @WithName("base-port")
        Optional<Integer> basePort();

        /**
         * TLS registry configuration name applied to generated clients.
         *
         * @return TLS configuration name
         */
        @WithName("tls-configuration-name")
        Optional<String> tlsConfigurationName();
    }

    /**
     * Health check configuration for startup dependency checks.
     */
    interface HealthConfig {
        /**
         * Maximum time to wait for startup health checks to complete.
         *
         * @return startup health check timeout
         */
        @WithDefault("PT5M")
        Duration startupTimeout();
    }

    /**
     * Cache configuration for pipeline cache providers.
     */
    interface CacheConfig {
        /**
         * Selects which cache provider to use (for example "redis", "caffeine", "memory").
         *
         * @return optional provider name
         */
        Optional<String> provider();

        /**
         * Cache policy for cache plugin behavior.
         *
         * @return cache policy
         */
        @WithDefault("prefer-cache")
        String policy();

        /**
         * Default cache TTL for cache plugin operations.
         *
         * @return optional TTL duration
         */
        Optional<Duration> ttl();

        /**
         * Configuration for the caffeine cache provider.
         *
         * @return caffeine cache configuration
         */
        CaffeineConfig caffeine();

        /**
         * Configuration for the redis cache provider.
         *
         * @return redis cache configuration
         */
        RedisConfig redis();
    }

    /**
     * Caffeine cache provider configuration.
     */
    interface CaffeineConfig {
        /**
         * Cache name used for Quarkus cache.
         *
         * @return cache name
         */
        @WithDefault("pipeline-cache")
        String name();

        /**
         * Maximum size for the cache.
         *
         * @return maximum cache size
         */
        @WithDefault("10000")
        long maximumSize();

        /**
         * Expire after write duration.
         *
         * @return optional expiration duration
         */
        Optional<Duration> expireAfterWrite();

        /**
         * Expire after access duration.
         *
         * @return optional expiration duration
         */
        Optional<Duration> expireAfterAccess();
    }

    /**
     * Redis cache provider configuration.
     */
    interface RedisConfig {
        /**
         * Prefix applied to cache keys in Redis.
         *
         * @return cache key prefix
         */
        @WithDefault("pipeline-cache:")
        String prefix();
    }
}
