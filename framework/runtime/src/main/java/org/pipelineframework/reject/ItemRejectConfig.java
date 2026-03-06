/*
 * Copyright (c) 2023-2026 Mariano Barcia
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

package org.pipelineframework.reject;

import java.util.Optional;

import io.quarkus.arc.Unremovable;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

/**
 * Item reject sink configuration.
 */
@ConfigMapping(prefix = "pipeline.item-reject")
@Unremovable
public interface ItemRejectConfig {

    /**
     * Item reject sink provider selector.
     *
     * @return provider name
     */
    @WithDefault("log")
    String provider();

    /**
     * Enables strict startup checks for the selected sink provider.
     *
     * @return true to fail startup on invalid sink provider configuration
     */
    @WithName("strict-startup")
    @WithDefault("true")
    boolean strictStartup();

    /**
     * Includes rejected payload in the envelope when true.
     *
     * @return true when payload capture is enabled
     */
    @WithName("include-payload")
    @WithDefault("false")
    boolean includePayload();

    /**
     * Maximum in-memory retained rejects for the memory provider.
     *
     * @return in-memory ring buffer capacity
     */
    @WithName("memory-capacity")
    @WithDefault("512")
    int memoryCapacity();

    /**
     * Behaviour when reject publication fails.
     *
     * @return publish failure policy
     */
    @WithName("publish-failure-policy")
    @WithDefault("CONTINUE")
    ItemRejectFailurePolicy publishFailurePolicy();

    /**
     * SQS provider-specific settings.
     *
     * @return SQS configuration
     */
    SqsConfig sqs();

    /**
     * SQS provider settings.
     */
    interface SqsConfig {

        /**
         * Queue URL used to publish reject envelopes.
         *
         * @return queue URL when configured
         */
        @WithName("queue-url")
        Optional<String> queueUrl();

        /**
         * Optional AWS region override.
         *
         * @return region when configured
         */
        @WithName("region")
        Optional<String> region();

        /**
         * Optional endpoint override, usually for local testing.
         *
         * @return endpoint override when configured
         */
        @WithName("endpoint-override")
        Optional<String> endpointOverride();
    }
}
