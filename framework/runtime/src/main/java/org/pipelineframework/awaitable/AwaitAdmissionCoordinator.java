package org.pipelineframework.awaitable;

import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import io.quarkus.runtime.StartupEvent;
import org.pipelineframework.awaitable.admission.AwaitAdmissionAcquireResult;
import org.pipelineframework.awaitable.admission.AwaitAdmissionOwner;
import org.pipelineframework.awaitable.admission.AwaitAdmissionReservation;
import org.pipelineframework.awaitable.admission.AwaitAdmissionScope;
import org.pipelineframework.awaitable.admission.AwaitAdmissionStore;
import org.pipelineframework.awaitable.spi.AwaitTransportAdapter;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;

/**
 * Applies the durable provider-facing pending budget before an interaction exists.
 */
@ApplicationScoped
public class AwaitAdmissionCoordinator {
    private static final String METADATA_SCOPE_PIPELINE = "tpf.await.admission.pipeline";
    private static final String METADATA_SCOPE_STEP = "tpf.await.admission.step";
    private static final String METADATA_SCOPE_ENDPOINT = "tpf.await.admission.endpoint";
    private static final String METADATA_OWNER = "tpf.await.admission.owner";
    private static final String METADATA_SLOT = "tpf.await.admission.slot";
    private static final String METADATA_EXPIRES_AT = "tpf.await.admission.expires_at";
    private static final String METADATA_LEASE_TOKEN = "tpf.await.admission.lease_token";
    public record AdmissionLease(
        AwaitAdmissionReservation reservation,
        boolean reused,
        boolean reconciledExpired,
        long waitMillis
    ) {
    }

    @Inject
    Instance<AwaitAdmissionStore> stores;

    @Inject
    Instance<AwaitTransportAdapter<?>> adapters;

    @Inject
    PipelineStepConfig stepConfig;

    @Inject
    PipelineConfig pipelineConfig;

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    PipelineReleaseIdentityResolver releaseIdentityResolver;

    private final Map<String, AwaitAdmissionReservation> reservationsByInteraction = new ConcurrentHashMap<>();

    public boolean enabled() {
        return stepConfig != null && stepConfig.awaitAdmission().enabled();
    }

    void validateStartup(@Observes StartupEvent event) {
        if (!enabled()) {
            return;
        }
        if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
            throw new IllegalStateException(
                "pipeline.await-admission.enabled requires pipeline.orchestrator.mode=QUEUE_ASYNC");
        }
        store();
    }

    public Uni<Optional<AdmissionLease>> acquire(
        AwaitStepDescriptor descriptor,
        String tenantId,
        String unitId,
        Integer itemIndex,
        String executionId,
        long expiresAtEpochMs
    ) {
        if (!enabled()) {
            return Uni.createFrom().item(Optional.empty());
        }
        if (orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
            return Uni.createFrom().failure(new IllegalStateException(
                "pipeline.await-admission.enabled requires pipeline.orchestrator.mode=QUEUE_ASYNC"));
        }
        AwaitTransportAdapter<?> adapter = adapter(descriptor.transportType());
        String endpoint = adapter.admissionEndpoint(descriptor).orElseThrow(() -> new IllegalStateException(
            "Await transport '" + descriptor.transportType() + "' does not define a durable admission endpoint"));
        AwaitAdmissionScope scope = new AwaitAdmissionScope(
            releaseIdentityResolver.pipelineId(orchestratorConfig), descriptor.stepId(), endpoint);
        AwaitAdmissionOwner owner = new AwaitAdmissionOwner(tenantId + ":" + unitId + ":" + (itemIndex == null ? "single" : itemIndex)
            + ":" + executionId);
        long startedAtEpochMs = System.currentTimeMillis();
        return acquireWhenAvailable(scope, owner, expiresAtEpochMs)
            .onItem().transform(result -> Optional.of(new AdmissionLease(
                result.reservation().orElseThrow(),
                result.reused(),
                result.reconciledExpired(),
                Math.max(0, System.currentTimeMillis() - startedAtEpochMs))));
    }

    public void bind(AwaitInteractionRecord interaction, AdmissionLease lease) {
        if (lease != null) {
            reservationsByInteraction.put(interaction.interactionId(), lease.reservation());
            AwaitCompletionMetrics.recordAdmissionAcquired(
                interaction,
                lease.reused(),
                lease.reconciledExpired(),
                lease.waitMillis());
        }
    }

    /**
     * Adds the reservation identity to durable dispatch metadata so a later runtime can release the exact slot.
     */
    public Map<String, Object> dispatchMetadata(AwaitInteractionRecord interaction, Map<String, Object> metadata) {
        Map<String, Object> result = new HashMap<>(metadata == null ? Map.of() : metadata);
        AwaitAdmissionReservation reservation = reservationsByInteraction.get(interaction.interactionId());
        if (reservation == null) {
            return Map.copyOf(result);
        }
        result.put(METADATA_SCOPE_PIPELINE, reservation.scope().pipelineId());
        result.put(METADATA_SCOPE_STEP, reservation.scope().stepId());
        result.put(METADATA_SCOPE_ENDPOINT, reservation.scope().endpoint());
        result.put(METADATA_OWNER, reservation.owner().key());
        result.put(METADATA_SLOT, reservation.slot());
        result.put(METADATA_EXPIRES_AT, reservation.expiresAtEpochMs());
        result.put(METADATA_LEASE_TOKEN, reservation.leaseToken());
        return Map.copyOf(result);
    }

    public Uni<Boolean> release(AwaitInteractionRecord interaction) {
        if (!enabled()) {
            return Uni.createFrom().item(false);
        }
        AwaitAdmissionReservation reservation = reservationsByInteraction.remove(interaction.interactionId());
        if (reservation == null) {
            return releaseRecoveredReservation(interaction);
        }
        return Uni.createFrom().completionStage(store().release(reservation))
            .invoke(released -> AwaitCompletionMetrics.recordAdmissionReleased(interaction, released));
    }

    /**
     * Discards a lease acquired before interaction persistence was definitely absent.
     */
    public Uni<Void> release(AwaitAdmissionReservation reservation) {
        return Uni.createFrom().completionStage(store().release(reservation)).replaceWithVoid();
    }

    private Uni<Boolean> releaseRecoveredReservation(AwaitInteractionRecord interaction) {
        Optional<AwaitAdmissionReservation> persisted = persistedReservation(interaction);
        if (persisted.isPresent()) {
            return Uni.createFrom().completionStage(store().release(persisted.orElseThrow()))
                .invoke(released -> AwaitCompletionMetrics.recordAdmissionReleased(interaction, released));
        }
        // A pre-token interaction cannot safely prove ownership of a slot after a restart:
        // the same owner may have reclaimed it with a newer lease. Keep the slot until its
        // expiry/reconciliation instead of allowing an old record to delete the new lease.
        return Uni.createFrom().item(false);
    }

    private static Optional<AwaitAdmissionReservation> persistedReservation(AwaitInteractionRecord interaction) {
        Map<String, Object> metadata = interaction.transportMetadata();
        try {
            Object pipeline = metadata.get(METADATA_SCOPE_PIPELINE);
            Object step = metadata.get(METADATA_SCOPE_STEP);
            Object endpoint = metadata.get(METADATA_SCOPE_ENDPOINT);
            Object owner = metadata.get(METADATA_OWNER);
            Object slot = metadata.get(METADATA_SLOT);
            Object expiresAt = metadata.get(METADATA_EXPIRES_AT);
            Object leaseToken = metadata.get(METADATA_LEASE_TOKEN);
            if (pipeline == null || step == null || endpoint == null || owner == null || slot == null || expiresAt == null || leaseToken == null) {
                return Optional.empty();
            }
            return Optional.of(new AwaitAdmissionReservation(
                new AwaitAdmissionScope(pipeline.toString(), step.toString(), endpoint.toString()),
                new AwaitAdmissionOwner(owner.toString()),
                Integer.parseInt(slot.toString()),
                Long.parseLong(expiresAt.toString()),
                leaseToken.toString()));
        } catch (IllegalArgumentException ignored) {
            return Optional.empty();
        }
    }

    private Uni<AwaitAdmissionAcquireResult> acquireWhenAvailable(
        AwaitAdmissionScope scope,
        AwaitAdmissionOwner owner,
        long expiresAtEpochMs
    ) {
        int capacity = Math.max(1, pipelineConfig.maxConcurrency());
        long now = System.currentTimeMillis();
        if (now >= expiresAtEpochMs) {
            return Uni.createFrom().failure(new IllegalStateException("Await admission deadline elapsed before a slot became available"));
        }
        return Uni.createFrom().completionStage(store().acquire(scope, owner, capacity, expiresAtEpochMs, now))
            .onItem().transformToUni(result -> result.reservation().isPresent()
                ? Uni.createFrom().item(result)
                : Uni.createFrom().voidItem()
                    .onItem().delayIt().by(Duration.ofMillis(Math.max(1, stepConfig.awaitAdmission().retryWaitMs())))
                    .chain(() -> acquireWhenAvailable(scope, owner, expiresAtEpochMs)));
    }

    private AwaitAdmissionStore store() {
        String configured = stepConfig.awaitAdmission().store().toLowerCase(Locale.ROOT);
        return stores.stream()
            .filter(store -> configured.equals(store.providerName().toLowerCase(Locale.ROOT)))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                "No AwaitAdmissionStore provider named '" + configured + "' is available"));
    }

    @SuppressWarnings("unchecked")
    private AwaitTransportAdapter<?> adapter(String transportType) {
        return adapters.stream()
            .filter(adapter -> adapter.type().equalsIgnoreCase(transportType))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No await transport adapter for type: " + transportType));
    }
}
