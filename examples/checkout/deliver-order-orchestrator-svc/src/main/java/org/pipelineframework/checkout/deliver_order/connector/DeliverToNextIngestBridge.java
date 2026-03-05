package org.pipelineframework.checkout.deliver_order.connector;

import com.google.protobuf.Message;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.pipelineframework.PipelineOutputBus;
import org.pipelineframework.checkout.common.connector.ConnectorUtils;
import org.pipelineframework.checkout.common.connector.IdempotencyGuard;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDeliveredSvc;

@ApplicationScoped
public class DeliverToNextIngestBridge {

    private static final Logger LOG = Logger.getLogger(DeliverToNextIngestBridge.class);

    private final PipelineOutputBus outputBus;
    private final DeliveredOrderForwardClient forwardClient;
    private final boolean enabled;
    private final boolean idempotencyEnabled;
    private final IdempotencyGuard idempotencyGuard;
    private final String backpressureStrategy;
    private final int backpressureBufferCapacity;
    private final Set<String> inFlightHandoffKeys = new HashSet<>();
    private final Object inFlightLock = new Object();

    private Cancellable forwardingSubscription;

    /**
     * Create a bridge that forwards delivered orders from the pipeline output to the next ingest step when enabled.
     *
     * @param outputBus the pipeline output bus to consume delivered-order events from
     * @param forwardClient the client used to forward delivered orders onward
     * @param enabled configuration flag; `true` enables forwarding, `false` disables it
     * @param idempotencyEnabled whether duplicate order ids should be filtered before forwarding
     * @param idempotencyMaxKeys max in-memory keys retained for duplicate filtering
     * @param backpressureStrategy overflow strategy for connector handoff (`BUFFER` or `DROP`)
     * @param backpressureBufferCapacity overflow buffer capacity when strategy is `BUFFER`
     */
    public DeliverToNextIngestBridge(
        PipelineOutputBus outputBus,
        DeliveredOrderForwardClient forwardClient,
        @ConfigProperty(name = "checkout.deliver.forward.enabled", defaultValue = "false") boolean enabled,
        @ConfigProperty(name = "checkout.deliver.forward.idempotency.enabled", defaultValue = "true")
        boolean idempotencyEnabled,
        @ConfigProperty(name = "checkout.deliver.forward.idempotency.max-keys", defaultValue = "10000")
        int idempotencyMaxKeys,
        @ConfigProperty(name = "checkout.deliver.forward.backpressure.strategy", defaultValue = "BUFFER")
        String backpressureStrategy,
        @ConfigProperty(name = "checkout.deliver.forward.backpressure.buffer-capacity", defaultValue = "256")
        int backpressureBufferCapacity
    ) {
        this.outputBus = Objects.requireNonNull(outputBus, "outputBus must not be null");
        this.forwardClient = Objects.requireNonNull(forwardClient, "forwardClient must not be null");
        this.enabled = enabled;
        this.idempotencyEnabled = idempotencyEnabled;
        int normalizedIdempotencyMaxKeys = idempotencyMaxKeys > 0 ? idempotencyMaxKeys : 10000;
        this.idempotencyGuard = idempotencyEnabled ? new IdempotencyGuard(normalizedIdempotencyMaxKeys) : null;
        this.backpressureStrategy = ConnectorUtils.normalizeBackpressureStrategy(backpressureStrategy);
        this.backpressureBufferCapacity = backpressureBufferCapacity > 0 ? backpressureBufferCapacity : 256;
    }

    /**
     * Starts the deliver-to-next-ingest forwarding bridge on application startup when configured enabled.
     *
     * Sets up a stream from the output bus, converts and filters delivered-order items, applies a retry
     * policy on failures, forwards the resulting stream via the forward client, and stores the resulting
     * subscription for later cancellation.
     *
     * @param ignored the startup event observed (unused)
     */
    void onStartup(@Observes StartupEvent ignored) {
        if (!enabled) {
            LOG.info("Deliver->next forwarding bridge disabled (checkout.deliver.forward.enabled=false)");
            return;
        }

        Multi<Object> sourceStream =
            ConnectorUtils.applyBackpressure(outputBus.stream(Object.class), backpressureStrategy, backpressureBufferCapacity);
        Multi<OrderDeliveredSvc.DeliveredOrder> deliveredStream = sourceStream
            // Use concatenate intentionally so mapping/idempotency reservation is serialized in this bridge.
            .onItem().transformToMulti(item -> {
                OrderDeliveredSvc.DeliveredOrder mapped = toDeliveredOrder(item);
                return mapped == null ? Multi.createFrom().empty() : Multi.createFrom().item(mapped);
            }).concatenate()
            .onItem().invoke(this::markForwarded)
            .onFailure().invoke(error -> {
                clearInFlightReservations();
                LOG.errorf(error, "Deliver->next stream failed before forwarding signature=%s",
                    ConnectorUtils.failureSignature(
                        "deliver-to-next",
                        "stream",
                        "downstream_ingest_failure",
                        "na",
                        "na"));
            })
            .onFailure().retry().withBackOff(Duration.ofMillis(100), Duration.ofSeconds(1)).indefinitely();

        forwardingSubscription = forwardClient.forward(deliveredStream);
        LOG.infof("Deliver->next forwarding bridge started using client %s", forwardClient.getClass().getName());
    }

    /**
     * Cancel the active forwarding subscription to the next ingest stage if present.
     *
     * <p>If a forwarding subscription exists, it is cancelled and a log entry is made; otherwise a
     * log entry notes that no subscription was active at shutdown.
     */
    @PreDestroy
    void onShutdown() {
        if (forwardingSubscription != null) {
            LOG.infof("Cancelling deliver->next forwarding subscription %s", forwardingSubscription);
            forwardingSubscription.cancel();
            LOG.info("Deliver->next forwarding subscription cancelled");
        } else {
            LOG.info("Deliver->next forwarding subscription was not active at shutdown");
        }
    }

    /**
     * Converts an input item to an OrderDeliveredSvc.DeliveredOrder when possible.
     *
     * If the input is already a DeliveredOrder it is returned as-is. If the input is a protobuf
     * Message and all required fields (order_id, customer_id, ready_at, dispatch_id, dispatched_at,
     * delivered_at) are present and non-blank, a new DeliveredOrder is constructed and returned.
     *
     * @return the converted OrderDeliveredSvc.DeliveredOrder, or `null` if the item is unsupported or required fields are missing
     */
    private OrderDeliveredSvc.DeliveredOrder toDeliveredOrder(Object item) {
        if (item instanceof OrderDeliveredSvc.DeliveredOrder delivered) {
            if (isDuplicateOrInFlight(delivered.getOrderId(), delivered.getDispatchId(), delivered.getDeliveredAt())) {
                return null;
            }
            return delivered;
        }
        if (item instanceof Message message) {
            String orderId = ConnectorUtils.readField(message, "order_id");
            String customerId = ConnectorUtils.readField(message, "customer_id");
            String readyAt = ConnectorUtils.readField(message, "ready_at");
            String dispatchId = ConnectorUtils.readField(message, "dispatch_id");
            String dispatchedAt = ConnectorUtils.readField(message, "dispatched_at");
            String deliveredAt = ConnectorUtils.readField(message, "delivered_at");
            if (!orderId.isBlank() && !customerId.isBlank() && !readyAt.isBlank()
                && !dispatchId.isBlank() && !dispatchedAt.isBlank() && !deliveredAt.isBlank()) {
                if (isDuplicateOrInFlight(orderId, dispatchId, deliveredAt)) {
                    return null;
                }
                return OrderDeliveredSvc.DeliveredOrder.newBuilder()
                    .setOrderId(orderId)
                    .setCustomerId(customerId)
                    .setReadyAt(readyAt)
                    .setDispatchId(dispatchId)
                    .setDispatchedAt(dispatchedAt)
                    .setDeliveredAt(deliveredAt)
                    .build();
            }
            LOG.debugf(
                "Dropped Message item due to missing required fields signature=%s",
                ConnectorUtils.failureSignature(
                    "deliver-to-next",
                    "mapping",
                    "missing_required_fields",
                    "na",
                    orderId));
            return null;
        }
        LOG.debugf(
            "Dropped unsupported deliver->next item signature=%s type=%s",
            ConnectorUtils.failureSignature(
                "deliver-to-next",
                "mapping",
                "unsupported_item_type",
                "na",
                "na"),
            item == null ? "null" : item.getClass().getName());
        return null;
    }

    private boolean isDuplicateOrInFlight(String orderId, String dispatchId, String deliveredAt) {
        if (!isIdempotencyActive() || orderId == null || orderId.isBlank()) {
            return false;
        }
        String handoffKey = handoffKey(orderId, dispatchId, deliveredAt);
        // Lock ordering invariant:
        // Always acquire inFlightLock before touching inFlightHandoffKeys and idempotencyGuard.
        // markForwarded follows the same order. Preserve this ordering in future changes.
        synchronized (inFlightLock) {
            if (idempotencyGuard.contains(handoffKey) || inFlightHandoffKeys.contains(handoffKey)) {
                LOG.debugf("Dropped duplicate deliver->next handoff orderId=%s handoffKey=%s", orderId, handoffKey);
                return true;
            }
            inFlightHandoffKeys.add(handoffKey);
        }
        return false;
    }

    private void markForwarded(OrderDeliveredSvc.DeliveredOrder order) {
        String orderId = order == null ? null : order.getOrderId();
        if (!isIdempotencyActive() || orderId == null || orderId.isBlank()) {
            return;
        }
        String handoffKey = handoffKey(orderId, order.getDispatchId(), order.getDeliveredAt());
        synchronized (inFlightLock) {
            inFlightHandoffKeys.remove(handoffKey);
            idempotencyGuard.markIfNew(handoffKey);
        }
    }

    private boolean isIdempotencyActive() {
        return idempotencyEnabled && idempotencyGuard != null;
    }

    private void clearInFlightReservations() {
        synchronized (inFlightLock) {
            // Intentionally retain idempotencyGuard state here:
            // this bridge currently enforces at-most-once semantics for forwarded orderIds.
            inFlightHandoffKeys.clear();
        }
    }

    private String handoffKey(String orderId, String dispatchId, String deliveredAt) {
        return ConnectorUtils.deterministicHandoffKey(
            "deliver-to-next",
            orderId,
            dispatchId,
            deliveredAt);
    }
}
