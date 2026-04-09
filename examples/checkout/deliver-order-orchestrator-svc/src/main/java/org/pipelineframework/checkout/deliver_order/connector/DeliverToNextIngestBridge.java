package org.pipelineframework.checkout.deliver_order.connector;

import com.google.protobuf.Message;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.pipelineframework.PipelineOutputBus;
import org.pipelineframework.checkout.common.connector.ConnectorUtils;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDeliveredSvc;

@ApplicationScoped
public class DeliverToNextIngestBridge {

    private static final Logger LOG = Logger.getLogger(DeliverToNextIngestBridge.class);

    private final PipelineOutputBus outputBus;
    private final DeliveredOrderForwardClient forwardClient;
    private final boolean enabled;
    private final boolean idempotencyEnabled;
    private final int idempotencyMaxKeys;
    private final String backpressureStrategy;
    private final int backpressureBufferCapacity;
    private final Set<String> acceptedKeys;
    private final Set<String> inFlightKeys;

    private Cancellable forwardingSubscription;

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
        this.idempotencyMaxKeys = normalizedIdempotencyMaxKeys;
        this.backpressureStrategy = ConnectorUtils.normalizeBackpressureStrategy(backpressureStrategy);
        this.backpressureBufferCapacity = backpressureBufferCapacity > 0 ? backpressureBufferCapacity : 256;
        this.acceptedKeys = idempotencyEnabled ? ConcurrentHashMap.newKeySet(normalizedIdempotencyMaxKeys) : Set.of();
        this.inFlightKeys = idempotencyEnabled ? ConcurrentHashMap.newKeySet(normalizedIdempotencyMaxKeys) : Set.of();
    }

    void onStartup(@Observes StartupEvent ignored) {
        if (!enabled) {
            LOG.info("Deliver->next forwarding bridge disabled (checkout.deliver.forward.enabled=false)");
            return;
        }

        Multi<OrderDeliveredSvc.DeliveredOrder> deliveredOrderStream = ConnectorUtils.applyBackpressure(
            outputBus.stream(Object.class),
            backpressureStrategy,
            backpressureBufferCapacity)
            .onItem().transformToMulti(item -> {
                OrderDeliveredSvc.DeliveredOrder mapped = toDeliveredOrder(item);
                return mapped == null
                    ? Multi.createFrom().empty()
                    : Multi.createFrom().item(mapped);
            }).concatenate()
            .select().where(this::reserveIfNeeded);

        forwardingSubscription = forwardClient.forward(
            deliveredOrderStream,
            this::markAccepted,
            this::handleForwardFailure);
        LOG.infof("Deliver->next forwarding bridge started using client %s", forwardClient.getClass().getName());
    }

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
            return delivered;
        }
        if (item instanceof Message message) {
            String orderId = ConnectorUtils.readField(message, "order_id");
            String customerId = ConnectorUtils.readField(message, "customer_id");
            String readyAt = ConnectorUtils.readField(message, "ready_at");
            String dispatchId = ConnectorUtils.readField(message, "dispatch_id");
            String dispatchedAt = ConnectorUtils.readField(message, "dispatched_at");
            String deliveredAt = ConnectorUtils.readField(message, "delivered_at");
            if (hasRequiredDeliveredFields(orderId, customerId, readyAt, dispatchId, dispatchedAt, deliveredAt)) {
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

    private boolean hasRequiredDeliveredFields(
        String orderId,
        String customerId,
        String readyAt,
        String dispatchId,
        String dispatchedAt,
        String deliveredAt
    ) {
        return !(orderId == null || orderId.isBlank()
            || customerId == null || customerId.isBlank()
            || readyAt == null || readyAt.isBlank()
            || dispatchId == null || dispatchId.isBlank()
            || dispatchedAt == null || dispatchedAt.isBlank()
            || deliveredAt == null || deliveredAt.isBlank());
    }

    int getIdempotencyMaxKeys() {
        return idempotencyMaxKeys;
    }

    String getBackpressureStrategy() {
        return backpressureStrategy;
    }

    int getBackpressureBufferCapacity() {
        return backpressureBufferCapacity;
    }

    private boolean reserveIfNeeded(OrderDeliveredSvc.DeliveredOrder deliveredOrder) {
        if (!idempotencyEnabled) {
            return true;
        }
        String key = handoffKey(deliveredOrder);
        if (key == null) {
            return true;
        }
        if (acceptedKeys.contains(key)) {
            LOG.debugf("Dropped duplicate deliver-to-next handoff idempotencyKey=%s", key);
            return false;
        }
        boolean reserved = inFlightKeys.add(key);
        if (!reserved) {
            LOG.debugf("Dropped in-flight duplicate deliver-to-next handoff idempotencyKey=%s", key);
        }
        return reserved;
    }

    private void markAccepted(OrderDeliveredSvc.DeliveredOrder deliveredOrder) {
        if (!idempotencyEnabled) {
            return;
        }
        String key = handoffKey(deliveredOrder);
        if (key == null) {
            return;
        }
        inFlightKeys.remove(key);
        acceptedKeys.add(key);
    }

    private void handleForwardFailure(Throwable failure) {
        if (idempotencyEnabled) {
            inFlightKeys.clear();
        }
        LOG.errorf(failure, "Deliver->next forwarding failed signature=%s",
            ConnectorUtils.failureSignature(
                "deliver-to-next",
                "forward",
                "downstream_ingest_failure",
                "na",
                "na"));
    }

    private String handoffKey(OrderDeliveredSvc.DeliveredOrder deliveredOrder) {
        if (deliveredOrder == null) {
            return null;
        }
        return ConnectorUtils.deterministicHandoffKey(
            "deliver-to-next",
            deliveredOrder.getOrderId(),
            deliveredOrder.getDispatchId(),
            deliveredOrder.getDeliveredAt());
    }
}
