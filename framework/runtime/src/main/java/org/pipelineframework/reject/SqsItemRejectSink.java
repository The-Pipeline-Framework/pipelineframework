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

    SqsItemRejectSink(SqsClient client, ItemRejectConfig itemRejectConfig) {
        this.client = client;
        this.itemRejectConfig = itemRejectConfig;
    }

    @Override
    public String providerName() {
        return "sqs";
    }

    @Override
    public int priority() {
        return -1000;
    }

    @Override
    public boolean durable() {
        return true;
    }

    @Override
    public Optional<String> startupValidationError(ItemRejectConfig config) {
        if (config == null || config.sqs().queueUrl().isEmpty() || config.sqs().queueUrl().get().isBlank()) {
            return Optional.of("pipeline.item-reject.sqs.queue-url must be configured when provider=sqs.");
        }
        return Optional.empty();
    }

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

    private static String toMessage(ItemRejectEnvelope envelope) {
        try {
            return PipelineJson.mapper().writeValueAsString(envelope);
        } catch (Exception e) {
            throw new IllegalStateException("Failed serializing item reject envelope for SQS publish.", e);
        }
    }

    private static <T> Uni<T> blocking(Supplier<T> supplier) {
        return Uni.createFrom().item(supplier).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }
}
