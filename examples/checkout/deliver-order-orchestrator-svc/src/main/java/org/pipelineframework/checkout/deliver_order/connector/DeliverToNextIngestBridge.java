package org.pipelineframework.checkout.deliver_order.connector;

import com.google.protobuf.Message;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.util.HashSet;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.pipelineframework.PipelineOutputBus;
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
    private final Set<String> inFlightOrderIds = new HashSet<>();

    private Cancellable forwardingSubscription;

    /**
     * Create a bridge that forwards delivered orders from the pipeline output to the next ingest step when enabled.
     *
     * @param outputBus the pipeline output bus to consume delivered-order events from
     * @param forwardClient the client used to forward delivered orders onward
     * @param enabled configuration flag; `true` enables forwarding, `false` disables it
     * @param idempotencyEnabled whether duplicate order ids should be filtered before forwarding
     * @param idempotencyMaxKeys max in-memory keys retained for duplicate filtering
     */
    public DeliverToNextIngestBridge(
        PipelineOutputBus outputBus,
        DeliveredOrderForwardClient forwardClient,
        @ConfigProperty(name = "checkout.deliver.forward.enabled", defaultValue = "false") boolean enabled,
        @ConfigProperty(name = "checkout.deliver.forward.idempotency.enabled", defaultValue = "true")
        boolean idempotencyEnabled,
        @ConfigProperty(name = "checkout.deliver.forward.idempotency.max-keys", defaultValue = "10000")
        int idempotencyMaxKeys
    ) {
        this.outputBus = Objects.requireNonNull(outputBus, "outputBus must not be null");
        this.forwardClient = Objects.requireNonNull(forwardClient, "forwardClient must not be null");
        this.enabled = enabled;
        this.idempotencyEnabled = idempotencyEnabled;
        this.idempotencyGuard = idempotencyEnabled ? new IdempotencyGuard(idempotencyMaxKeys) : null;
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

        Multi<OrderDeliveredSvc.DeliveredOrder> deliveredStream = outputBus.stream(Object.class)
            .onItem().transformToMulti(item -> {
                OrderDeliveredSvc.DeliveredOrder mapped = toDeliveredOrder(item);
                return mapped == null ? Multi.createFrom().empty() : Multi.createFrom().item(mapped);
            }).concatenate()
            .onItem().invoke(order -> markForwarded(order.getOrderId()))
            .onFailure().invoke(error -> {
                clearInFlightReservations();
                LOG.error("Deliver->next stream failed before forwarding", error);
            })
            .onFailure().retry().withBackOff(Duration.ofMillis(100), Duration.ofSeconds(1)).atMost(5);

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
            if (isDuplicateOrInFlight(delivered.getOrderId())) {
                return null;
            }
            return delivered;
        }
        if (item instanceof Message message) {
            String orderId = readField(message, "order_id");
            String customerId = readField(message, "customer_id");
            String readyAt = readField(message, "ready_at");
            String dispatchId = readField(message, "dispatch_id");
            String dispatchedAt = readField(message, "dispatched_at");
            String deliveredAt = readField(message, "delivered_at");
            if (!orderId.isBlank() && !customerId.isBlank() && !readyAt.isBlank()
                && !dispatchId.isBlank() && !dispatchedAt.isBlank() && !deliveredAt.isBlank()) {
                if (isDuplicateOrInFlight(orderId)) {
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
                "Dropped Message item due to missing required fields order_id='%s' customer_id='%s' ready_at='%s' dispatch_id='%s' dispatched_at='%s' delivered_at='%s'",
                orderId, customerId, readyAt, dispatchId, dispatchedAt, deliveredAt);
            return null;
        }
        LOG.debugf(
            "Dropped unsupported deliver->next item type=%s",
            item == null ? "null" : item.getClass().getName());
        return null;
    }

    private boolean isDuplicateOrInFlight(String orderId) {
        if (!idempotencyEnabled || orderId == null || orderId.isBlank()) {
            return false;
        }
        if (idempotencyGuard == null) {
            return false;
        }
        synchronized (inFlightOrderIds) {
            if (idempotencyGuard.contains(orderId) || inFlightOrderIds.contains(orderId)) {
                LOG.debugf("Dropped duplicate deliver->next handoff orderId=%s", orderId);
                return true;
            }
            inFlightOrderIds.add(orderId);
        }
        return false;
    }

    private void markForwarded(String orderId) {
        if (!idempotencyEnabled || orderId == null || orderId.isBlank() || idempotencyGuard == null) {
            return;
        }
        synchronized (inFlightOrderIds) {
            inFlightOrderIds.remove(orderId);
            idempotencyGuard.markIfNew(orderId);
        }
    }

    private void clearInFlightReservations() {
        synchronized (inFlightOrderIds) {
            inFlightOrderIds.clear();
        }
    }

    /**
     * Retrieves a field's textual representation from a protobuf Message.
     *
     * If the named field is present and its Java type is STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN,
     * or ENUM this returns the field's string form; if the field is missing or its type is
     * BYTE_STRING or MESSAGE this returns an empty string.
     *
     * @param message the protobuf Message to read from
     * @param fieldName the name of the field to read
     * @return the field value as a string, or an empty string when the field is not found or not representable as text
     */
    private static String readField(Message message, String fieldName) {
        var field = message.getDescriptorForType().findFieldByName(fieldName);
        if (field == null) {
            return "";
        }
        Object value = message.getField(field);
        return switch (field.getJavaType()) {
            case STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, ENUM -> String.valueOf(value);
            case BYTE_STRING, MESSAGE -> "";
        };
    }
}
