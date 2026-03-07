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

import java.net.URI;
import java.util.Optional;
import java.util.function.Supplier;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.jboss.logging.Logger;
import org.pipelineframework.config.pipeline.PipelineJson;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * SQS-backed item reject sink provider.
 */
@ApplicationScoped
public class SqsItemRejectSink implements ItemRejectSink {

    private static final Logger LOG = Logger.getLogger(SqsItemRejectSink.class);

    @Inject
    ItemRejectConfig itemRejectConfig;

    private volatile SqsClient client;
    private volatile boolean shuttingDown;

    /**
     * Default constructor for CDI.
     */
    public SqsItemRejectSink() {
    }

    /**
     * Package-private constructor for supplying a pre-built SQS client and item-reject configuration,
     * primarily used for dependency injection and testing.
     *
     * @param client the SqsClient to use for sending messages; may be null to allow lazy initialization
     * @param itemRejectConfig configuration containing SQS settings for item rejection
     */
    SqsItemRejectSink(SqsClient client, ItemRejectConfig itemRejectConfig) {
        this.client = client;
        this.itemRejectConfig = itemRejectConfig;
    }

    /**
     * Identifies the sink provider used by this implementation.
     *
     * @return the provider name "sqs"
     */
    @Override
    public String providerName() {
        return "sqs";
    }

    /**
     * The provider's selection priority used to order sinks when multiple providers exist.
     *
     * @return the priority value; -1000 indicates a low selection priority among providers
     */
    @Override
    public int priority() {
        return -1000;
    }

    /**
     * Indicates that this sink is durable and intended to persist item-reject events for delivery.
     *
     * @return `true` if the sink is durable, `false` otherwise
     */
    @Override
    public boolean durable() {
        return true;
    }

    /**
     * Validates that the SQS queue URL is configured in the provided item-reject configuration.
     *
     * @param config the item-reject configuration to validate; if `null` it is treated as missing configuration
     * @return an `Optional` containing an error message if the SQS queue URL is missing or blank, otherwise an empty `Optional`
     */
    @Override
    public Optional<String> startupValidationError(ItemRejectConfig config) {
        if (config == null || config.sqs().queueUrl().isEmpty() || config.sqs().queueUrl().get().isBlank()) {
            return Optional.of("pipeline.item-reject.sqs.queue-url must be configured when provider=sqs.");
        }
        return Optional.empty();
    }

    /**
     * Sends the given item-reject envelope to the configured SQS queue and records publish metrics.
     *
     * @param envelope the item rejection envelope to serialize and send
     * @return a Uni containing no value that completes when the send operation finishes
     * @throws IllegalStateException if the SQS queue URL is not configured, if the sink is shutting down,
     *                               or if the envelope cannot be serialized to JSON
     */
    @Override
    public Uni<Void> publish(ItemRejectEnvelope envelope) {
        String queueUrl = itemRejectConfig.sqs().queueUrl()
            .filter(url -> !url.isBlank())
            .orElseThrow(() -> new IllegalStateException(
                "pipeline.item-reject.sqs.queue-url must be configured when provider=sqs."));
        String messageBody = toMessage(envelope);
        return blocking(() -> {
            sqsClient().sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .build());
            return null;
        }).replaceWithVoid().invoke(() -> ItemRejectMetrics.record(providerName(), envelope));
    }

    /**
     * Shut down the SQS client and mark this sink as shutting down.
     *
     * This method is synchronized, idempotent, and safe to call multiple times. It sets the
     * internal shutdown flag, attempts to close the active SqsClient, suppresses and logs any
     * exception at debug level, and clears the client reference.
     *
     * Called during container shutdown to release resources.
     */
    @PreDestroy
    void closeClient() {
        synchronized (this) {
            shuttingDown = true;
            SqsClient active = client;
            if (active == null) {
                return;
            }
            try {
                active.close();
            } catch (Exception e) {
                LOG.debug("Failed closing SQS client for item reject sink.", e);
            } finally {
                client = null;
            }
        }
    }

    /**
     * Lazily initializes (if necessary) and returns the SqsClient used to send messages.
     *
     * The client is created once and cached; when building, the configured region and
     * endpointOverride are applied if present and non-blank.
     *
     * @return the initialized SqsClient instance
     * @throws IllegalStateException if the sink is shutting down when called
     */
    private SqsClient sqsClient() {
        if (shuttingDown) {
            throw new IllegalStateException("SqsItemRejectSink is shutting down.");
        }
        SqsClient active = client;
        if (active != null) {
            return active;
        }
        synchronized (this) {
            if (shuttingDown) {
                throw new IllegalStateException("SqsItemRejectSink is shutting down.");
            }
            if (client == null) {
                var builder = SqsClient.builder();
                itemRejectConfig.sqs().region()
                    .filter(region -> !region.isBlank())
                    .ifPresent(region -> builder.region(Region.of(region)));
                itemRejectConfig.sqs().endpointOverride()
                    .filter(endpoint -> !endpoint.isBlank())
                    .ifPresent(endpoint -> builder.endpointOverride(URI.create(endpoint)));
                client = builder.build();
            }
            return client;
        }
    }

    /**
     * Serialize an ItemRejectEnvelope to its JSON string representation.
     *
     * @param envelope the envelope to serialize
     * @return the JSON string representation of the envelope
     * @throws IllegalStateException if serialization fails (wraps the original cause)
     */
    private static String toMessage(ItemRejectEnvelope envelope) {
        try {
            return PipelineJson.mapper().writeValueAsString(envelope);
        } catch (Exception e) {
            throw new IllegalStateException("Failed serializing item reject envelope for SQS publish.", e);
        }
    }

    /**
     * Execute a blocking supplier on the Mutiny default worker pool and produce its result.
     *
     * @param <T> the type of the supplier result
     * @param supplier the blocking computation to run on the worker pool
     * @return the value produced by the supplier, emitted by the returned Uni
     */
    private static <T> Uni<T> blocking(Supplier<T> supplier) {
        return Uni.createFrom().item(supplier).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }
}
