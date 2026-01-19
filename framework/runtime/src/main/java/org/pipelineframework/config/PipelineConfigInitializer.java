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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import io.quarkus.runtime.StartupEvent;
import org.jboss.logging.Logger;

/**
 * Initializes the PipelineConfig with values from the application configuration.
 * This ensures that the global pipeline defaults defined in application.properties
 * are properly applied to the PipelineConfig.
 */
@ApplicationScoped
public class PipelineConfigInitializer {

    private static final Logger logger = Logger.getLogger(PipelineConfigInitializer.class);

    @Inject
    PipelineConfig pipelineConfig;

    @Inject
    PipelineStepConfig stepConfig;

    /**
     * Default constructor for PipelineConfigInitializer.
     */
    public PipelineConfigInitializer() {
    }

    /**
     * Initializes the PipelineConfig with values from application configuration
     * when the application starts up.
     */
    void onStart(@Observes StartupEvent event) {
        logger.debug("Initializing PipelineConfig with application configuration defaults");

        // Get the defaults from the configuration system
        PipelineStepConfig.StepConfig config = stepConfig.defaults();

        logger.info("Initializing pipeline global/default configuration");
        logger.infof("Parallelism policy: %s", stepConfig.parallelism());
        logger.infof("Max concurrency: %s", stepConfig.maxConcurrency());
        logger.infof("Retry limit: %s", config.retryLimit());
        logger.infof("Retry wait: %s ms", config.retryWaitMs());
        logger.infof("Backpressure buffer capacity: %s", config.backpressureBufferCapacity());
        logger.infof("Backpressure strategy: %s", config.backpressureStrategy());
        logger.infof("Jitter: %s", config.jitter());
        logger.infof("Max backoff: %s ms", config.maxBackoff());
        logger.infof("Recover on failure: %s", config.recoverOnFailure());

        // Apply these values to the PipelineConfig
        StepConfig defaults = pipelineConfig.defaults()
                .retryLimit(config.retryLimit())
                .retryWait(Duration.ofMillis(config.retryWaitMs()))
                .recoverOnFailure(config.recoverOnFailure())
                .maxBackoff(Duration.ofMillis(config.maxBackoff()))
                .jitter(config.jitter())
                .backpressureBufferCapacity(config.backpressureBufferCapacity())
                .backpressureStrategy(config.backpressureStrategy());

        pipelineConfig.parallelism(stepConfig.parallelism());
        pipelineConfig.maxConcurrency(stepConfig.maxConcurrency());

        logger.info("Pipeline configuration loaded from Quarkus config system");
    }
}
