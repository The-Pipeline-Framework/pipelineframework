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

package org.pipelineframework.pipeline;

import io.smallrye.mutiny.Uni;
import java.util.concurrent.atomic.AtomicInteger;
import org.jboss.logging.Logger;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;

public class RetryTestSteps {

    private static final Logger LOG = Logger.getLogger(RetryTestSteps.class);

    public static class AsyncFailNTimesStep extends ConfigurableStep
            implements StepOneToOne<String, String> {
        private final int failCount;
        private final AtomicInteger callCount = new AtomicInteger(0);

        public AsyncFailNTimesStep(int failCount) {
            this.failCount = failCount;
        }

        @Override
        public Uni<String> applyOneToOne(String input) {
            if (callCount.incrementAndGet() <= failCount) {
                throw new RuntimeException("Intentional async failure #" + callCount.get());
            }
            return Uni.createFrom().item("Async Success: " + input);
        }

        private boolean hasManualConfig = false;
        private int manualRetryLimit = -1;
        private java.time.Duration manualRetryWait = null;
        private boolean manualRecoverOnFailure = false;

        /**
         * Log the dead-letter event and recover the original input.
         *
         * @param failedItem a Uni containing the item that failed processing
         * @param cause the exception that caused the failure
         * @return the original input string contained in {@code failedItem}
         */
        @Override
        public Uni<String> deadLetter(Uni<String> failedItem, Throwable cause) {
            LOG.infof(
                    "AsyncFailedNTimesStep dead letter: %s due to %s",
                    failedItem, cause.getMessage());
            // For recovery, return the original input value from the Uni
            return failedItem.onItem().transform(item -> item);
        }

        /**
         * Initialises the step from the supplied StepConfig and preserves the first observed
         * non-default retry-related values as manual overrides for subsequent initialisations.
         *
         * <p>
         * If a non-null `config` is provided and this is the first time non-default values for
         * `retryLimit`, `retryWait` or `recoverOnFailure` are observed, those values are stored as
         * manual overrides. When manual overrides exist they are applied on top of any later
         * `config` passed to this method.
         *
         * @param config the configuration to apply; may be {@code null}. Values are compared
         *        against a fresh {@code StepConfig} instance to detect non-defaults.
         */
        @Override
        public void initialiseWithConfig(org.pipelineframework.config.StepConfig config) {
            // Check if this is the first time being configured with non-default values
            // If so, preserve these as manual configuration
            org.pipelineframework.config.StepConfig defaults = new org.pipelineframework.config.StepConfig();
            if (!hasManualConfig && config != null
                    && (config.retryLimit() != defaults.retryLimit()
                            || config.retryWait() != defaults.retryWait()
                            || config.recoverOnFailure() != defaults.recoverOnFailure())) {
                // This looks like manual configuration - save the values
                setManualConfig(
                        config.retryLimit(), config.retryWait(), config.recoverOnFailure());
            }

            if (hasManualConfig) {
                // If we have manual config, apply it on top of the new config
                super.initialiseWithConfig(config);
                // Apply the manual overrides
                if (config != null) {
                    config.retryLimit(manualRetryLimit)
                            .retryWait(manualRetryWait)
                            .recoverOnFailure(manualRecoverOnFailure);
                }
            } else {
                super.initialiseWithConfig(config);
            }
        }

        /**
         * Record that manual retry configuration has been provided and store its values.
         *
         * @param retryLimit the maximum number of retry attempts to apply
         * @param retryWait the wait duration between retry attempts
         * @param recoverOnFailure whether the step should recover and emit the original value after
         *        failures
         */
        private void setManualConfig(
                int retryLimit, java.time.Duration retryWait, boolean recoverOnFailure) {
            this.hasManualConfig = true;
            this.manualRetryLimit = retryLimit;
            this.manualRetryWait = retryWait;
            this.manualRecoverOnFailure = recoverOnFailure;
        }

        public int getCallCount() {
            return callCount.get();
        }
    }
}
