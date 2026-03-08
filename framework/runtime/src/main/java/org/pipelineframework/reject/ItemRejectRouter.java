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

    /**
     * Constructs an ItemRejectRouter with the given configuration, available sinks, and optional launch-mode override.
     *
     * @param itemRejectConfig configuration that controls reject publishing behaviour
     * @param pipelineStepConfig pipeline step configuration used to determine recover-on-failure settings
     * @param itemRejectSinks provider collection of available ItemRejectSink implementations
     * @param launchModeOverride optional launch mode to use instead of the runtime default (may be null)
     */
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

    /**
     * Observes application startup and initializes the item reject router.
     *
     * @param event the startup event that triggers initialization
     */
    void onStart(@Observes StartupEvent event) {
        initialize();
    }

    /**
     * Initialize and select the configured ItemRejectSink, validate its startup compatibility with
     * configuration, enforce recover-on-failure durability constraints, and set the active sink.
     *
     * <p>If the selected sink reports a startup validation error and strict startup is enabled,
     * initialization will fail. If recover-on-failure is configured but the selected sink is not
     * durable, initialization will fail when running in production mode; otherwise a warning is logged.
     * On success, the selected sink is stored for subsequent use.
     *
     * @throws IllegalStateException if a startup validation error occurs and strict startup is enabled,
     *                               or if recover-on-failure is enabled with a non-durable sink while
     *                               running in production mode
     */
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
     * Publish metadata for a single rejected item to the configured item-reject sink.
     *
     * @param stepClass the processing step class where the rejection occurred (may be null)
     * @param item the rejected item payload
     * @param error the failure that caused the rejection
     * @param retriesObserved number of retry attempts observed before the final failure (may be null)
     * @param retryLimit configured retry limit for the step (may be null)
     * @param <O> step output type
     * @return `null` of the step's output type on successful completion
     */
    public <O> Uni<O> publishItemReject(
        Class<?> stepClass,
        Object item,
        Throwable error,
        Integer retriesObserved,
        Integer retryLimit
    ) {
        return publishItemReject(
            stepClass,
            item,
            error,
            retriesObserved,
            retryLimit,
            TransportDispatchMetadataHolder.get(),
            PipelineContextHolder.get());
    }

    /**
     * Publish metadata for a single rejected item using explicitly provided transport/context metadata.
     *
     * @param stepClass the processing step class where the rejection occurred (may be null)
     * @param item the rejected item payload
     * @param error the failure that caused the rejection
     * @param retriesObserved number of retry attempts observed before the final failure (may be null)
     * @param retryLimit configured retry limit for the step (may be null)
     * @param transport captured dispatch metadata (preferred over thread-local lookup; may be null)
     * @param context captured pipeline context (preferred over thread-local lookup; may be null)
     * @param <O> step output type
     * @return `null` of the step's output type on successful completion
     */
    public <O> Uni<O> publishItemReject(
        Class<?> stepClass,
        Object item,
        Throwable error,
        Integer retriesObserved,
        Integer retryLimit,
        TransportDispatchMetadata transport,
        PipelineContext context
    ) {
        ItemRejectEnvelope envelope = buildEnvelope(
            stepClass,
            SCOPE_ITEM,
            item,
            1L,
            error,
            retriesObserved,
            retryLimit,
            transport,
            context);
        return publishEnvelope(envelope).chain(() -> Uni.createFrom().nullItem());
    }

    /**
     * Publish a rejection summary for a failed stream.
     *
     * Builds and publishes an ItemRejectEnvelope that contains a sample of items and the total
     * item count for the stream, along with error and retry metadata.
     *
     * @param stepClass the step's implementation class (may be null)
     * @param sampleItems a sample of items from the failed stream; may be null, treated as an empty list
     * @param totalItemCount total number of items seen in the stream; negative values are treated as zero
     * @param error the failure cause (may be null)
     * @param retriesObserved number of retries observed before final failure; may be null
     * @param retryLimit configured retry limit; may be null
     * @param <O> step output type
     * @return `null` cast to the step's output type when publishing completes
     */
    public <O> Uni<O> publishStreamReject(
        Class<?> stepClass,
        List<?> sampleItems,
        long totalItemCount,
        Throwable error,
        Integer retriesObserved,
        Integer retryLimit
    ) {
        return publishStreamReject(
            stepClass,
            sampleItems,
            totalItemCount,
            error,
            retriesObserved,
            retryLimit,
            TransportDispatchMetadataHolder.get(),
            PipelineContextHolder.get());
    }

    /**
     * Publish a rejection summary for a failed stream using explicitly provided transport/context metadata.
     *
     * @param stepClass the step's implementation class (may be null)
     * @param sampleItems a sample of items from the failed stream; may be null, treated as an empty list
     * @param totalItemCount total number of items seen in the stream; negative values are treated as zero
     * @param error the failure cause (may be null)
     * @param retriesObserved number of retries observed before final failure; may be null
     * @param retryLimit configured retry limit; may be null
     * @param transport captured dispatch metadata (preferred over thread-local lookup; may be null)
     * @param context captured pipeline context (preferred over thread-local lookup; may be null)
     * @param <O> step output type
     * @return `null` cast to the step's output type when publishing completes
     */
    public <O> Uni<O> publishStreamReject(
        Class<?> stepClass,
        List<?> sampleItems,
        long totalItemCount,
        Throwable error,
        Integer retriesObserved,
        Integer retryLimit,
        TransportDispatchMetadata transport,
        PipelineContext context
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
            retryLimit,
            transport,
            context);
        return publishEnvelope(envelope).chain(() -> Uni.createFrom().nullItem());
    }

    /**
     * Publishes an ItemRejectEnvelope through the selected sink while applying the configured failure policy.
     *
     * @param envelope the item reject envelope to publish
     * @return `void` on successful publication; if the configured failure policy is `CONTINUE`, publish failures are logged and the returned completion succeeds normally, otherwise (policy `FAIL_PIPELINE`) publish failures are propagated. 
     */
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

    /**
     * Build an ItemRejectEnvelope containing transport and pipeline context, step metadata, retry information, error details, timestamp, fingerprint, and optional payload/item count.
     *
     * @param stepClass       the step's class; used to record step class name and simple name (may be null)
     * @param scope           rejection scope, typically "ITEM" or "STREAM"
     * @param payloadSource   original object used for payload and fingerprint calculation; payload may be omitted depending on configuration
     * @param itemCount       total number of items for stream rejects; ignored for item-level rejects
     * @param error           the failure that caused the reject; if null, a placeholder error is recorded
     * @param retriesObserved number of retries already observed (may be null)
     * @param retryLimit      configured retry limit (may be null)
     * @return an ItemRejectEnvelope populated with transport identifiers, pipeline replay mode, step information, scope, retry metrics, error class and message, event timestamp, fingerprint, optional item count (for streams), and optional payload
     */
    private ItemRejectEnvelope buildEnvelope(
        Class<?> stepClass,
        String scope,
        Object payloadSource,
        Long itemCount,
        Throwable error,
        Integer retriesObserved,
        Integer retryLimit,
        TransportDispatchMetadata providedTransport,
        PipelineContext providedContext
    ) {
        // Prefer metadata captured upstream before reactive thread hops; fallback to thread-local lookup.
        TransportDispatchMetadata transport = providedTransport == null
            ? TransportDispatchMetadataHolder.get()
            : providedTransport;
        PipelineContext context = providedContext == null
            ? PipelineContextHolder.get()
            : providedContext;

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

    /**
     * Ensure the configured ItemRejectSink is initialized and return it.
     *
     * Initializes the sink lazily in a thread-safe manner if it has not been set.
     *
     * @return the initialized ItemRejectSink instance
     */
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

    /**
     * Selects the ItemRejectSink implementation that matches the configured provider name and has the highest priority.
     *
     * @param providerName configured provider name; if null or blank, any available provider is considered a match
     * @return the matching ItemRejectSink with the highest priority
     * @throws IllegalStateException if no matching ItemRejectSink provider is found
     */
    private ItemRejectSink selectSink(String providerName) {
        return itemRejectSinks.stream()
            .filter(provider -> providerMatches(provider.providerName(), providerName))
            .sorted((left, right) -> Integer.compare(right.priority(), left.priority()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                "No ItemRejectSink provider found for '" + providerName + "'."));
    }

    /**
     * Determines whether recover-on-failure is enabled for the pipeline.
     *
     * Checks the global defaults first; if not enabled there, returns `true` when any step override
     * explicitly enables recover-on-failure.
     *
     * @return `true` if defaults.enable recover-on-failure or any step override sets recover-on-failure to `true`, `false` otherwise
     */
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

    /**
     * Indicates whether the application is running in production (normal) launch mode.
     *
     * @return true if the resolved launch mode is {@link io.quarkus.runtime.LaunchMode#NORMAL}, false otherwise.
     */
    private boolean isProductionLaunchMode() {
        return launchMode() == LaunchMode.NORMAL;
    }

    /**
     * Resolve the effective launch mode for this router.
     *
     * @return the configured launch mode override if present; otherwise the current runtime launch mode,
     *         or `LaunchMode.NORMAL` if the runtime mode cannot be determined
     */
    private LaunchMode launchMode() {
        if (launchModeOverride != null) {
            return launchModeOverride;
        }
        try {
            return LaunchMode.current();
        } catch (IllegalStateException expected) {
            return LaunchMode.NORMAL;
        } catch (RuntimeException unexpected) {
            LOG.warn("Unable to resolve runtime launch mode. Defaulting to NORMAL.", unexpected);
            return LaunchMode.NORMAL;
        }
    }

    /**
     * Determines whether a provider's name matches the configured provider filter.
     *
     * @param availableName  the provider name available from a sink implementation
     * @param configuredName the configured provider name filter; if null or blank, treated as a wildcard that matches any provider
     * @return `true` if `configuredName` is null/blank or equals `availableName` ignoring case, `false` otherwise
     */
    private static boolean providerMatches(String availableName, String configuredName) {
        if (configuredName == null || configuredName.isBlank()) {
            return true;
        }
        return configuredName.equalsIgnoreCase(availableName);
    }

    /**
     * Produces a stable fingerprint for the given payload.
     *
     * Attempts to serialize the payload to JSON and returns the SHA-256 digest as a hex string.
     * If JSON serialization fails, computes the SHA-256 digest of payload.toString() in UTF-8.
     * If digest computation fails, returns the hexadecimal representation of payload.hashCode().
     *
     * @param payload the object to fingerprint (may be null)
     * @return a hex string representing the payload fingerprint
     */
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
