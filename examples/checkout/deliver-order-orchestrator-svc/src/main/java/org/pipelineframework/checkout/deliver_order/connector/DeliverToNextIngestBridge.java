package org.pipelineframework.checkout.deliver_order.connector;

import com.google.protobuf.Message;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.subscription.Cancellable;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.pipelineframework.PipelineOutputBus;
import org.pipelineframework.connector.ConnectorFailureMode;
import org.pipelineframework.connector.ConnectorIdempotencyPolicy;
import org.pipelineframework.connector.ConnectorIdempotencyTracker;
import org.pipelineframework.connector.ConnectorPolicy;
import org.pipelineframework.connector.ConnectorRecord;
import org.pipelineframework.connector.ConnectorRuntime;
import org.pipelineframework.connector.ConnectorSupport;
import org.pipelineframework.connector.ConnectorTarget;
import org.pipelineframework.connector.OutputBusConnectorSource;
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
    private final ConnectorIdempotencyTracker idempotencyTracker;
    private final String backpressureStrategy;
    private final int backpressureBufferCapacity;

    private Cancellable forwardingSubscription;

    /**
     * Create a bridge that forwards delivered orders from the pipeline output to the next ingest step when enabled.
     *
     * @param outputBus the pipeline output bus to consume delivered-order events from
     * @param forwardClient client used to forward delivered orders to the next ingest step
     * @param enabled whether forwarding is enabled
     * @param idempotencyEnabled whether idempotency filtering of duplicates is enabled
     * @param idempotencyMaxKeys maximum number of in-memory keys retained for idempotency filtering
     * @param backpressureStrategy strategy to apply when the connector handoff overflows (e.g., BUFFER or DROP)
     * @param backpressureBufferCapacity buffer capacity used when the backpressure strategy is BUFFER
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
        this.idempotencyMaxKeys = normalizedIdempotencyMaxKeys;
        this.idempotencyTracker = idempotencyEnabled ? new ConnectorIdempotencyTracker(normalizedIdempotencyMaxKeys) : null;
        this.backpressureStrategy = ConnectorUtils.normalizeBackpressureStrategy(backpressureStrategy);
        this.backpressureBufferCapacity = backpressureBufferCapacity > 0 ? backpressureBufferCapacity : 256;
    }

    /**
     * Start the forwarding bridge that forwards delivered orders to the next ingest stage when enabled.
     *
     * If forwarding is disabled the method returns without action. When enabled, it subscribes to the pipeline output,
     * converts stream items to DeliveredOrder instances, begins forwarding them via the configured forward client,
     * and stores the resulting subscription for later cancellation.
     *
     * @param ignored the observed startup event (unused)
     */
    void onStartup(@Observes StartupEvent ignored) {
        if (!enabled) {
            LOG.info("Deliver->next forwarding bridge disabled (checkout.deliver.forward.enabled=false)");
            return;
        }

        ConnectorRuntime<Object, OrderDeliveredSvc.DeliveredOrder> runtime = new ConnectorRuntime<>(
            "deliver-to-next",
            new OutputBusConnectorSource<>(outputBus, Object.class),
            connectorTarget(),
            this::mapRecord,
            new ConnectorPolicy(
                true,
                ConnectorSupport.normalizeBackpressurePolicy(backpressureStrategy),
                backpressureBufferCapacity,
                idempotencyEnabled ? ConnectorIdempotencyPolicy.ON_ACCEPT : ConnectorIdempotencyPolicy.DISABLED,
                ConnectorFailureMode.PROPAGATE),
            idempotencyTracker,
            rejected -> LOG.debugf(
                "Rejected deliver-to-next handoff payloadType=%s",
                rejected == null || rejected.payload() == null ? "null" : rejected.payload().getClass().getName()),
            duplicate -> LOG.debugf(
                "Dropped duplicate deliver-to-next handoff idempotencyKey=%s",
                duplicate == null ? null : duplicate.idempotencyKey()),
            failure -> LOG.errorf(failure, "Deliver->next forwarding failed signature=%s",
                ConnectorUtils.failureSignature(
                    "deliver-to-next",
                    "forward",
                    "downstream_ingest_failure",
                    "na",
                    "na")));

        forwardingSubscription = runtime.start();
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

    /**
     * Determine whether all required delivered-order fields contain non-blank text.
     *
     * @param orderId      the order identifier
     * @param customerId   the customer identifier
     * @param readyAt      the ready timestamp/value
     * @param dispatchId   the dispatch identifier
     * @param dispatchedAt the dispatched timestamp/value
     * @param deliveredAt  the delivered timestamp/value
     * @return             true if all parameters are non-null and contain non-blank text, false otherwise
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

    int getIdempotencyMaxKeys() {
        return idempotencyMaxKeys;
    }

    String getBackpressureStrategy() {
        return backpressureStrategy;
    }

    int getBackpressureBufferCapacity() {
        return backpressureBufferCapacity;
    }

    private ConnectorRecord<OrderDeliveredSvc.DeliveredOrder> mapRecord(ConnectorRecord<Object> sourceRecord) {
        OrderDeliveredSvc.DeliveredOrder mapped = toDeliveredOrder(sourceRecord.payload());
        if (mapped == null) {
            return null;
        }
        return ConnectorRecord.ofPayload(
            mapped,
            ConnectorSupport.ensureDispatchMetadata(
                sourceRecord.dispatchMetadata(),
                "deliver-to-next",
                mapped,
                List.of("orderId", "dispatchId", "deliveredAt")),
            Map.of(
                "connector.name", "deliver-to-next",
                "connector.source.step", "Order Delivered",
                "connector.target.pipeline", "next-ingest",
                "connector.contract", mapped.getDescriptorForType().getFullName()));
    }

    private ConnectorTarget<OrderDeliveredSvc.DeliveredOrder> connectorTarget() {
        return new ConnectorTarget<>() {
            @Override
            public Cancellable forward(io.smallrye.mutiny.Multi<ConnectorRecord<OrderDeliveredSvc.DeliveredOrder>> connectorStream) {
                return forward(connectorStream, ignored -> {
                }, ignored -> {
                });
            }

            @Override
            public Cancellable forward(
                io.smallrye.mutiny.Multi<ConnectorRecord<OrderDeliveredSvc.DeliveredOrder>> connectorStream,
                Consumer<ConnectorRecord<OrderDeliveredSvc.DeliveredOrder>> onAccepted,
                Consumer<Throwable> onFailure
            ) {
                ConcurrentMap<String, ConnectorRecord<OrderDeliveredSvc.DeliveredOrder>> pending = new ConcurrentHashMap<>();
                return forwardClient.forward(
                    connectorStream.onItem().invoke(record -> {
                        String key = record.idempotencyKey();
                        if (key != null && !key.isBlank()) {
                            pending.put(key, record);
                        }
                    }).onItem().transform(ConnectorRecord::payload),
                    payload -> {
                        String key = ConnectorSupport.deriveIdempotencyKey(
                            "deliver-to-next",
                            payload,
                            List.of("orderId", "dispatchId", "deliveredAt"));
                        ConnectorRecord<OrderDeliveredSvc.DeliveredOrder> accepted = key == null ? null : pending.remove(key);
                        onAccepted.accept(accepted == null
                            ? mapRecord(ConnectorRecord.<Object>ofPayload(payload))
                            : accepted);
                    },
                    failure -> {
                        pending.clear();
                        onFailure.accept(failure);
                    });
            }
        };
    }
}
