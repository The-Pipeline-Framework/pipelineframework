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
     * Selects which item reject sink provider to use.
     *
     * @return the provider name (for example "log")
     */
    @WithDefault("log")
    String provider();

    /**
     * Controls whether the application fails startup when the selected sink provider is misconfigured.
     *
     * @return true if startup should fail when the selected sink provider configuration is invalid, false otherwise.
     */
    @WithName("strict-startup")
    @WithDefault("true")
    boolean strictStartup();

    /**
     * Controls whether the rejected item's payload is included in the reject envelope.
     *
     * @return true if the rejected payload is included in the envelope, false otherwise.
     */
    @WithName("include-payload")
    @WithDefault("false")
    boolean includePayload();

    /**
     * Maximum number of reject envelopes retained in memory by the memory provider.
     *
     * @return the in-memory ring buffer capacity (maximum retained rejects)
     */
    @WithName("memory-capacity")
    @WithDefault("512")
    int memoryCapacity();

    /**
     * Determines behavior when publishing a rejected item fails.
     *
     * @return the policy that controls what happens if reject publication fails
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
         * SQS queue URL used to publish reject envelopes.
         *
         * @return an Optional containing the configured SQS queue URL, or empty if none is configured
         */
        @WithName("queue-url")
        Optional<String> queueUrl();

        /**
         * AWS region override to use when publishing to SQS.
         *
         * @return an Optional containing the region if configured, otherwise an empty Optional
         */
        @WithName("region")
        Optional<String> region();

        /**
         * Endpoint override for the SQS provider, typically used for local testing or custom endpoints.
         *
         * @return the configured endpoint override, if present
         */
        @WithName("endpoint-override")
        Optional<String> endpointOverride();
    }
}
