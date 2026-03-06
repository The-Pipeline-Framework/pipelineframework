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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.quarkus.runtime.LaunchMode;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.context.TransportDispatchMetadata;
import org.pipelineframework.context.TransportDispatchMetadataHolder;

/**
 * Central router for step-level reject publication.
 */
@ApplicationScoped
public class ItemRejectRouter {

    private static final Logger LOG = Logger.getLogger(ItemRejectRouter.class);
    private static final String SCOPE_ITEM = "ITEM";
    private static final String SCOPE_STREAM = "STREAM";

    @Inject
    ItemRejectConfig itemRejectConfig;

    @Inject
    PipelineStepConfig pipelineStepConfig;

    @Inject
    Instance<ItemRejectSink> itemRejectSinks;

    private volatile ItemRejectSink sink;
    private LaunchMode launchModeOverride;

    /**
     * Default constructor for CDI.
     */
    public ItemRejectRouter() {
    }

    ItemRejectRouter(
        ItemRejectConfig itemRejectConfig,
        PipelineStepConfig pipelineStepConfig,
        Instance<ItemRejectSink> itemRejectSinks,
        LaunchMode launchModeOverride
    ) {
        this.itemRejectConfig = itemRejectConfig;
        this.pipelineStepConfig = pipelineStepConfig;
        this.itemRejectSinks = itemRejectSinks;
        this.launchModeOverride = launchModeOverride;
    }

    void onStart(@Observes StartupEvent event) {
        initialize();
    }

    void initialize() {
        ItemRejectSink selected = selectSink(itemRejectConfig.provider());
        selected.startupValidationError(itemRejectConfig).ifPresent(message -> {
            if (itemRejectConfig.strictStartup()) {
                throw new IllegalStateException(message);
            }
            LOG.warn(message);
        });

        boolean recoveryEnabled = recoverOnFailureConfigured();
        if (recoveryEnabled && !selected.durable()) {
            String message = "Item reject sink provider '" + selected.providerName()
                + "' is non-durable while recoverOnFailure=true. Configure pipeline.item-reject.provider=sqs for production.";
            if (isProductionLaunchMode()) {
                throw new IllegalStateException(message);
            }
            LOG.warn(message);
        }

        sink = selected;
        LOG.infof("Item reject sink enabled: provider=%s durable=%s includePayload=%s failurePolicy=%s",
            sink.providerName(),
            sink.durable(),
            itemRejectConfig.includePayload(),
            itemRejectConfig.publishFailurePolicy());
    }

    /**
     * Publishes one rejected item.
     *
     * @param stepClass step class
     * @param item rejected item
     * @param error failure cause
     * @param retriesObserved retries observed before final failure
     * @param retryLimit configured retry limit
     * @param <O> step output type
     * @return completion as null item of output type
     */
    public <O> Uni<O> publishItemReject(
        Class<?> stepClass,
        Object item,
        Throwable error,
        Integer retriesObserved,
        Integer retryLimit
    ) {
        ItemRejectEnvelope envelope = buildEnvelope(
            stepClass,
            SCOPE_ITEM,
            item,
            1L,
            error,
            retriesObserved,
            retryLimit);
        return publishEnvelope(envelope).replaceWith((O) null);
    }

    /**
     * Publishes one rejected stream summary.
     *
     * @param stepClass step class
     * @param sampleItems sample items collected from the stream
     * @param totalItemCount total items seen in the stream
     * @param error failure cause
     * @param retriesObserved retries observed before final failure
     * @param retryLimit configured retry limit
     * @param <O> step output type
     * @return completion as null item of output type
     */
    public <O> Uni<O> publishStreamReject(
        Class<?> stepClass,
        List<?> sampleItems,
        long totalItemCount,
        Throwable error,
        Integer retriesObserved,
        Integer retryLimit
    ) {
        Object payload = Map.of(
            "sample", sampleItems == null ? List.of() : List.copyOf(sampleItems),
            "totalCount", Math.max(0L, totalItemCount));
        ItemRejectEnvelope envelope = buildEnvelope(
            stepClass,
            SCOPE_STREAM,
            payload,
            Math.max(0L, totalItemCount),
            error,
            retriesObserved,
            retryLimit);
        return publishEnvelope(envelope).replaceWith((O) null);
    }

    private Uni<Void> publishEnvelope(ItemRejectEnvelope envelope) {
        ItemRejectSink selected = ensureSinkInitialized();
        Uni<Void> publish = selected.publish(envelope);
        if (itemRejectConfig.publishFailurePolicy() == ItemRejectFailurePolicy.FAIL_PIPELINE) {
            return publish;
        }
        return publish
            .onFailure().invoke(failure -> LOG.errorf(
                failure,
                "Item reject sink publish failed with CONTINUE policy: provider=%s step=%s scope=%s",
                selected.providerName(),
                envelope.stepClass(),
                envelope.rejectScope()))
            .onFailure().recoverWithNull()
            .replaceWithVoid();
    }

    private ItemRejectEnvelope buildEnvelope(
        Class<?> stepClass,
        String scope,
        Object payloadSource,
        Long itemCount,
        Throwable error,
        Integer retriesObserved,
        Integer retryLimit
    ) {
        TransportDispatchMetadata transport = TransportDispatchMetadataHolder.get();
        PipelineContext context = PipelineContextHolder.get();

        Throwable normalizedError = error == null
            ? new IllegalStateException("Unknown reject failure")
            : error;
        String stepClassName = stepClass == null ? "unknown" : stepClass.getName();
        String stepName = stepClass == null ? "unknown" : stepClass.getSimpleName();

        Object payload = itemRejectConfig.includePayload() ? payloadSource : null;
        String fingerprint = fingerprint(payloadSource);
        int observed = retriesObserved == null ? 0 : Math.max(0, retriesObserved);
        int computedFinalAttempt = observed + 1;

        return new ItemRejectEnvelope(
            null,
            transport == null ? null : transport.executionId(),
            transport == null ? null : transport.correlationId(),
            transport == null ? null : transport.idempotencyKey(),
            context == null ? null : context.replayMode(),
            stepClassName,
            stepName,
            scope,
            transport == null ? null : transport.retryAttempt(),
            observed,
            retryLimit,
            computedFinalAttempt,
            normalizedError.getClass().getName(),
            normalizedError.getMessage(),
            Instant.now().toEpochMilli(),
            fingerprint,
            SCOPE_STREAM.equals(scope) ? itemCount : null,
            payload);
    }

    private ItemRejectSink ensureSinkInitialized() {
        ItemRejectSink selected = sink;
        if (selected != null) {
            return selected;
        }
        synchronized (this) {
            if (sink == null) {
                initialize();
            }
            return sink;
        }
    }

    private ItemRejectSink selectSink(String providerName) {
        return itemRejectSinks.stream()
            .filter(provider -> providerMatches(provider.providerName(), providerName))
            .sorted((left, right) -> Integer.compare(right.priority(), left.priority()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                "No ItemRejectSink provider found for '" + providerName + "'."));
    }

    private boolean recoverOnFailureConfigured() {
        if (pipelineStepConfig == null) {
            return false;
        }
        boolean defaultsRecover = Boolean.TRUE.equals(pipelineStepConfig.defaults().recoverOnFailure());
        if (defaultsRecover) {
            return true;
        }
        Map<String, PipelineStepConfig.StepConfig> stepOverrides = pipelineStepConfig.step();
        if (stepOverrides == null || stepOverrides.isEmpty()) {
            return false;
        }
        return stepOverrides.values().stream()
            .anyMatch(stepConfig -> Boolean.TRUE.equals(stepConfig.recoverOnFailure()));
    }

    private boolean isProductionLaunchMode() {
        return launchMode() == LaunchMode.NORMAL;
    }

    private LaunchMode launchMode() {
        if (launchModeOverride != null) {
            return launchModeOverride;
        }
        try {
            return LaunchMode.current();
        } catch (Exception ignored) {
            return LaunchMode.NORMAL;
        }
    }

    private static boolean providerMatches(String availableName, String configuredName) {
        if (configuredName == null || configuredName.isBlank()) {
            return true;
        }
        return configuredName.equalsIgnoreCase(availableName);
    }

    private static String fingerprint(Object payload) {
        try {
            byte[] bytes = PipelineJson.mapper().writeValueAsBytes(payload);
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(bytes));
        } catch (Exception ignored) {
            String fallback = String.valueOf(payload);
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                return HexFormat.of().formatHex(digest.digest(fallback.getBytes(StandardCharsets.UTF_8)));
            } catch (Exception digestFailure) {
                return Integer.toHexString(fallback.hashCode());
            }
        }
    }
}
