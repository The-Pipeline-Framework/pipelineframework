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

import java.util.Map;

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
 * To configure pipeline order: pipeline.order=fully.qualified.StepClass1,fully.qualified.StepClass2
 */
@ConfigMapping(prefix = "pipeline")
@Unremovable
public interface PipelineStepConfig {

    /**
     * Specifies the global execution order of pipeline steps.
     *
     * <p>This ordered list contains fully-qualified step class names that determine which steps run and their sequence.
     *
     * @return the ordered list of fully-qualified step class names to execute in the pipeline
     */
    @WithDefault(" ")
    java.util.List<String> order();

    /**
 * Default configuration applied to every pipeline step.
 *
 * When used as global defaults, properties are applied to every step unless a step provides overrides; the
 * `order` property is ignored in the global defaults.
 *
 * @return the StepConfig containing default values for pipeline steps
 */
    StepConfig defaults();
    
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
     * Configuration for individual pipeline steps, allowing per-step override of global defaults.
     */
    interface StepConfig {
        /**
         * Execution order of this step within the pipeline.
         * <p>
         * When this value is used in global defaults (pipeline.defaults) it is ignored; order is only
         * meaningful for per-step configuration.
         *
         * @return the step order as an integer; `0` if not specified for a specific step.
         */
        @WithDefault("0")
        Integer order();

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
         * Whether the step processes items in parallel.
         *
         * @return true if parallel processing is enabled, false otherwise.
         */
        @WithDefault("false")
        Boolean parallel();

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
}