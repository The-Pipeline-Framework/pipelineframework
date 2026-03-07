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
     * Start the forwarding bridge that relays delivered orders to the next ingest stage when forwarding is enabled.
     *
     * Creates a stream from the pipeline output, maps and filters items into DeliveredOrder instances, applies
     * backpressure and idempotency reservation, retries on failures with exponential backoff, forwards the stream
     * via the forward client, and stores the resulting subscription for later cancellation.
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
     * Produces an OrderDeliveredSvc.DeliveredOrder from an input item when possible.
     *
     * If the item is already a DeliveredOrder it is validated and returned. If the item is a protobuf
     * Message the required fields order_id, customer_id, ready_at, dispatch_id, dispatched_at, and
     * delivered_at are extracted and used to build a DeliveredOrder. Converts only when all required
     * fields are present and the order is not considered a duplicate or already in-flight.
     *
     * @param item the input object to convert (may be a DeliveredOrder or a protobuf Message)
     * @return the converted OrderDeliveredSvc.DeliveredOrder, or null if the item is unsupported,
     *         required fields are missing, or the order is duplicate/in-flight
     */
    private OrderDeliveredSvc.DeliveredOrder toDeliveredOrder(Object item) {
        if (item instanceof OrderDeliveredSvc.DeliveredOrder delivered) {
            String orderId = delivered.getOrderId();
            if (!hasRequiredDeliveredFields(
                orderId,
                delivered.getCustomerId(),
                delivered.getReadyAt(),
                delivered.getDispatchId(),
                delivered.getDispatchedAt(),
                delivered.getDeliveredAt()
            )) {
                LOG.debugf(
                    "Dropped DeliveredOrder item due to missing required fields signature=%s",
                    ConnectorUtils.failureSignature(
                        "deliver-to-next",
                        "mapping",
                        "missing_required_fields",
                        "na",
                        orderId));
                return null;
            }
            if (isDuplicateOrInFlight(orderId, delivered.getDispatchId(), delivered.getDeliveredAt())) {
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

    /**
     * Checks whether a delivered order is a duplicate or already being forwarded, and reserves a handoff key when not.
     *
     * <p>If idempotency is inactive or `orderId` is blank, the method returns `false` and does not reserve anything.
     *
     * @param orderId   the order identifier used as primary idempotency input
     * @param dispatchId the dispatch identifier included in the handoff key
     * @param deliveredAt the delivery timestamp included in the handoff key
     * @return `true` if the derived handoff key has already been seen or is currently reserved (duplicate or in-flight), `false` otherwise
     */
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

    /**
     * Record that an order's handoff key has been forwarded and clear its in-flight reservation.
     *
     * If idempotency is active and the order has a non-blank orderId, computes the handoff key
     * from the order's orderId, dispatchId, and deliveredAt, removes that key from the in-flight
     * reservations, and marks the key as seen in the idempotency guard.
     *
     * @param order the delivered order whose handoff key should be marked forwarded; if `null`
     *              or the orderId is missing/blank the method has no effect
     */
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

    /**
     * Releases all current in-flight handoff reservations so those items may be retried or reprocessed.
     *
     * <p>Idempotency guard state is intentionally preserved so keys already recorded as forwarded remain treated as delivered.
     * This method synchronizes on {@code inFlightLock} while clearing {@code inFlightHandoffKeys}.
     */
    private void clearInFlightReservations() {
        synchronized (inFlightLock) {
            // Intentionally retain idempotencyGuard state here:
            // this bridge currently enforces at-most-once semantics for forwarded orderIds.
            inFlightHandoffKeys.clear();
        }
    }

    /**
     * Create a deterministic handoff key used for idempotency and in-flight tracking.
     *
     * @param orderId    the order identifier to include in the key
     * @param dispatchId the dispatch identifier to include in the key
     * @param deliveredAt the delivery timestamp to include in the key
     * @return a deterministic string key derived from the provided identifiers and the "deliver-to-next" context
     */
    private String handoffKey(String orderId, String dispatchId, String deliveredAt) {
        return ConnectorUtils.deterministicHandoffKey(
            "deliver-to-next",
            orderId,
            dispatchId,
            deliveredAt);
    }

    /**
     * Checks that all required delivered-order fields are present and contain non-blank text.
     *
     * @param orderId     the order identifier
     * @param customerId  the customer identifier
     * @param readyAt     the ready timestamp/value
     * @param dispatchId  the dispatch identifier
     * @param dispatchedAt the dispatched timestamp/value
     * @param deliveredAt the delivered timestamp/value
     * @return            `true` if all parameters are non-null and not blank, `false` otherwise
     */
    private boolean hasRequiredDeliveredFields(String orderId, String customerId, String readyAt,
                                               String dispatchId, String dispatchedAt, String deliveredAt) {
        return !(orderId == null || orderId.isBlank()
            || customerId == null || customerId.isBlank()
            || readyAt == null || readyAt.isBlank()
            || dispatchId == null || dispatchId.isBlank()
            || dispatchedAt == null || dispatchedAt.isBlank()
            || deliveredAt == null || deliveredAt.isBlank());
    }
}
