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

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepManyToMany;
import org.pipelineframework.step.StepOneToMany;
import org.pipelineframework.step.StepOneToOne;

@ApplicationScoped
public class TestSteps {

    private static final Logger LOG = Logger.getLogger(TestSteps.class);

    public static class TestStepOneToOneProcessed extends ConfigurableStep
            implements StepOneToOne<String, String> {
        private boolean hasManualConfig = false;
        private int manualRetryLimit = -1;
        private java.time.Duration manualRetryWait = null;

        /**
         * Process an input string with a short blocking simulation.
         *
         * @param input the string to process
         * @return the processed string prefixed with "Processed: "
         */
        @Override
        public Uni<String> applyOneToOne(String input) {
            // This is a blocking operation that simulates processing
            try {
                Thread.sleep(10); // Simulate some work
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return Uni.createFrom().item("Processed: " + input);
        }

        /**
         * Initialises the step with the provided configuration, preserving any first non-default
         * values as manual overrides.
         *
         * <p>
         * If this is the first time a non-default configuration is supplied, the method records
         * the supplied `retryLimit` and `retryWait` as manual configuration. Once manual
         * configuration exists, those stored values are applied to any subsequent incoming `config`
         * before delegating to the superclass initialisation.
         *
         * @param config the step configuration to apply; may be {@code null}
         */
        @Override
        public void initialiseWithConfig(org.pipelineframework.config.StepConfig config) {
            // Check if this is the first time being configured with non-default values
            // If so, preserve these as manual configuration
            if (!hasManualConfig && config != null) {
                // Check if the incoming config has custom values
                final org.pipelineframework.config.StepConfig defaultCfg = new org.pipelineframework.config.StepConfig();
                if (config.retryLimit() != defaultCfg.retryLimit()
                        || !java.util.Objects.equals(config.retryWait(), defaultCfg.retryWait())) {
                    // This looks like manual configuration - save the values
                    setManualConfig(config.retryLimit(), config.retryWait());
                }
            }

            if (hasManualConfig) {
                if (config != null) {
                    config.retryLimit(manualRetryLimit).retryWait(manualRetryWait);
                }
                super.initialiseWithConfig(config);
            } else {
                super.initialiseWithConfig(config);
            }
        }

        /**
         * Record manual retry configuration and mark the step as manually configured.
         *
         * @param retryLimit the manual retry limit to apply
         * @param retryWait the manual wait duration between retries to apply
         */
        public void setManualConfig(int retryLimit, java.time.Duration retryWait) {
            this.hasManualConfig = true;
            this.manualRetryLimit = retryLimit;
            this.manualRetryWait = retryWait;
        }

        /**
         * Gets the retry limit applied to this step.
         *
         * @return the maximum number of retry attempts allowed by the effective configuration
         */
        public int retryLimit() {
            return effectiveConfig().retryLimit();
        }

        /**
         * Get the wait duration used between retry attempts from the effective step configuration.
         *
         * @return the configured wait duration between retry attempts
         */
        public java.time.Duration retryWait() {
            return effectiveConfig().retryWait();
        }

    }

    public static class TestStepOneToMany extends ConfigurableStep
            implements StepOneToMany<String, String> {
        @Override
        public Multi<String> applyOneToMany(String input) {
            return Multi.createFrom().items(input + "-1", input + "-2", input + "-3");
        }
    }

    public static class TestStepManyToMany extends ConfigurableStep
            implements StepManyToMany<Object, Object> {
        @Override
        public Multi<Object> applyTransform(Multi<Object> input) {
            return input.onItem().transform(item -> "Streamed: " + item);
        }
    }

    public static class TestStepOneToOne extends ConfigurableStep
            implements StepOneToOne<String, String> {
        @Override
        public Uni<String> applyOneToOne(String input) {
            return Uni.createFrom().item("Async: " + input);
        }
    }

    public static class FailingStepOneToOne extends ConfigurableStep
            implements StepOneToOne<String, String> {
        // Configuration preservation fields like in AsyncFailNTimesStep
        private boolean hasManualConfig = false;
        private int manualRetryLimit = -1;
        private java.time.Duration manualRetryWait = null;
        private boolean manualRecoverOnFailure = false;
        private boolean manualRecoverOnFailureSet = false; // Sentinel to track if constructor set the value

        public FailingStepOneToOne() {
            // Leave recoverOnFailure to be driven by configuration unless explicitly set
        }

        public FailingStepOneToOne(boolean shouldRecover) {
            this.manualRecoverOnFailure = shouldRecover;
            this.manualRecoverOnFailureSet = true; // Mark that constructor explicitly set this value
        }

        /**
         * Produces a failed Uni for the given input to simulate a processing failure so the item can be handled by rejectItem.
         *
         * @param input the original item; preserved for downstream reject handling
         * @return a `Uni` that fails with a `RuntimeException`, allowing recovery logic to observe the original input
         */
        @Override
        public Uni<String> applyOneToOne(String input) {
            // Return the input wrapped in a Uni that fails - this way the input is preserved
            // for potential recovery by the rejectItem method
            return Uni.createFrom()
                    .failure(new RuntimeException("Intentional failure for testing"));
        }

        /**
         * Log an item-reject event for a failed input and emit the original item.
         *
         * @param failedItem a Uni that produces the input value which failed processing
         * @param cause the throwable that caused the failure
         * @return the original input value
         */
        @Override
        public Uni<String> rejectItem(Uni<String> failedItem, Throwable cause) {
            return failedItem
                    .onItem()
                    .invoke(
                            item -> LOG.infof(
                                    "Item reject handled for item: %s, cause: %s",
                                    item, cause.getMessage()));
        }

        /**
         * Initialize the step's configuration, preserving the first-seen non-default values as manual overrides.
         *
         * <p>If this is the first non-default configuration seen by the step, the method records the
         * incoming `retryLimit`, `retryWait`, and `recoverOnFailure` (subject to constructor precedence)
         * as manual configuration. Once manual configuration is recorded, subsequent calls apply the
         * recorded `retryLimit`, `retryWait`, and `recoverOnFailure` to the provided config before
         * delegating to the superclass initializer.
         *
         * @param config the step configuration to apply; may be null
         */
        @Override
        public void initialiseWithConfig(org.pipelineframework.config.StepConfig config) {
            // Check if this is the first time being configured with non-default values
            // If so, preserve these as manual configuration (like AsyncFailNTimesStep)
            if (!hasManualConfig && config != null) {
                // Check if the incoming config has custom values
                final org.pipelineframework.config.StepConfig defaultCfg = new org.pipelineframework.config.StepConfig();
                boolean hasConfigRecoverOnFailure = config.recoverOnFailure() != defaultCfg.recoverOnFailure();
                if (config.retryLimit() != defaultCfg.retryLimit()
                        || !java.util.Objects.equals(config.retryWait(), defaultCfg.retryWait())
                        || hasConfigRecoverOnFailure) {
                    // This looks like manual configuration - save the values
                    // Only set recoverOnFailure from config if constructor didn't set it
                    boolean recoverOnFailureToUse = manualRecoverOnFailureSet
                            ? manualRecoverOnFailure
                            : config.recoverOnFailure();
                    setManualConfig(config.retryLimit(), config.retryWait(), recoverOnFailureToUse);
                }
            }

            if (hasManualConfig) {
                if (config != null) {
                    config.retryLimit(manualRetryLimit)
                            .retryWait(manualRetryWait)
                            .recoverOnFailure(manualRecoverOnFailure);
                }
                super.initialiseWithConfig(config);
            } else {
                if (config != null) {
                    // Only apply manual recoverOnFailure if it was explicitly set
                    if (manualRecoverOnFailureSet) {
                        config.recoverOnFailure(manualRecoverOnFailure);
                    }
                    // Otherwise, leave config as-is (do nothing)
                }
                super.initialiseWithConfig(config);
            }
        }

        /**
         * Store manual retry configuration and optional recover-on-failure preference for the step.
         *
         * If the constructor did not explicitly set the recover-on-failure policy, the supplied
         * `recoverOnFailure` value is recorded; otherwise the constructor-provided policy remains in effect.
         *
         * @param retryLimit the manual retry limit to apply
         * @param retryWait the manual duration to wait between retries
         * @param recoverOnFailure whether failed items should be recovered instead of being item-rejected;
         *        ignored if the constructor previously fixed this behavior
         */
        private void setManualConfig(
                int retryLimit, java.time.Duration retryWait, boolean recoverOnFailure) {
            this.hasManualConfig = true;
            this.manualRetryLimit = retryLimit;
            this.manualRetryWait = retryWait;
            // Only update manualRecoverOnFailure if it wasn't set by constructor
            if (!manualRecoverOnFailureSet) {
                this.manualRecoverOnFailure = recoverOnFailure;
            }
        }
    }
}
