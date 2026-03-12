package org.pipelineframework.checkout.create_order.connector;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.google.protobuf.Message;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.subscription.Cancellable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.jboss.logging.Logger;
import org.pipelineframework.connector.ConnectorFailureMode;
import org.pipelineframework.connector.ConnectorIdempotencyPolicy;
import org.pipelineframework.connector.ConnectorIdempotencyTracker;
import org.pipelineframework.connector.ConnectorPolicy;
import org.pipelineframework.connector.ConnectorRecord;
import org.pipelineframework.connector.ConnectorRuntime;
import org.pipelineframework.connector.ConnectorSupport;
import org.pipelineframework.connector.GrpcIngestConnectorTarget;
import org.pipelineframework.connector.OutputBusConnectorSource;
import org.pipelineframework.checkout.common.connector.ConnectorUtils;
import org.pipelineframework.PipelineOutputBus;
import org.pipelineframework.checkout.createorder.grpc.OrderReadySvc;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDispatchSvc;

/**
 * Bridges checkpoint outputs from CreateOrder pipeline to DeliverOrder pipeline via gRPC ingest.
 */
@ApplicationScoped
public class CreateToDeliverIngestBridge {

    private static final Logger LOG = Logger.getLogger(CreateToDeliverIngestBridge.class);

    private final PipelineOutputBus outputBus;
    private final DeliverOrderIngestClient deliverOrderIngestClient;
    private final boolean idempotencyEnabled;
    private final ConnectorIdempotencyTracker idempotencyTracker;
    private final String backpressureStrategy;
    private final int backpressureBufferCapacity;

    private Cancellable forwardingSubscription;

    /**
     * Constructs a bridge that forwards CreateOrder pipeline checkpoint outputs to the DeliverOrder ingest client.
     *
     * Initializes optional in-memory idempotency tracking and normalizes backpressure strategy and buffer capacity.
     *
     * @param outputBus the source of pipeline checkpoint outputs; must not be null
     * @param deliverOrderIngestClient the gRPC client used to forward ready orders; must not be null
     * @param idempotencyEnabled if true, enables in-memory idempotency tracking to filter duplicate order IDs
     * @param idempotencyMaxKeys maximum number of order ID keys retained by the idempotency tracker; if non-positive, 10000 is used
     * @param backpressureStrategy overflow strategy for connector handoff (normalized; common values include {@code BUFFER} or {@code DROP})
     * @param backpressureBufferCapacity buffer capacity used when the backpressure strategy is {@code BUFFER}; if non-positive, 256 is used
     * @throws NullPointerException if {@code outputBus} or {@code deliverOrderIngestClient} is null
     */
    public CreateToDeliverIngestBridge(
        PipelineOutputBus outputBus,
        DeliverOrderIngestClient deliverOrderIngestClient,
        @ConfigProperty(name = "checkout.create-to-deliver.idempotency.enabled", defaultValue = "true")
        boolean idempotencyEnabled,
        @ConfigProperty(name = "checkout.create-to-deliver.idempotency.max-keys", defaultValue = "10000")
        int idempotencyMaxKeys,
        @ConfigProperty(name = "checkout.create-to-deliver.backpressure.strategy", defaultValue = "BUFFER")
        String backpressureStrategy,
        @ConfigProperty(name = "checkout.create-to-deliver.backpressure.buffer-capacity", defaultValue = "256")
        int backpressureBufferCapacity
    ) {
        this.outputBus = Objects.requireNonNull(outputBus, "outputBus must not be null");
        this.deliverOrderIngestClient =
            Objects.requireNonNull(deliverOrderIngestClient, "deliverOrderIngestClient must not be null");
        this.idempotencyEnabled = idempotencyEnabled;
        int normalizedIdempotencyMaxKeys = idempotencyMaxKeys > 0 ? idempotencyMaxKeys : 10000;
        this.idempotencyTracker = idempotencyEnabled ? new ConnectorIdempotencyTracker(normalizedIdempotencyMaxKeys) : null;
        this.backpressureStrategy = ConnectorUtils.normalizeBackpressureStrategy(backpressureStrategy);
        this.backpressureBufferCapacity = backpressureBufferCapacity > 0 ? backpressureBufferCapacity : 256;
    }

    /**
     * Start forwarding ReadyOrder events from the pipeline output bus to the DeliverOrder ingest client.
     *
     * Initializes and starts the connector runtime, stores the resulting cancellable subscription,
     * and logs bridge startup.
     */
    void onStartup(@Observes StartupEvent ignored) {
        ConnectorRuntime<Object, OrderDispatchSvc.ReadyOrder> runtime = new ConnectorRuntime<>(
            "create-to-deliver",
            new OutputBusConnectorSource<>(outputBus, Object.class),
            new GrpcIngestConnectorTarget<>(deliverOrderIngestClient::forward),
            this::mapRecord,
            new ConnectorPolicy(
                true,
                ConnectorSupport.normalizeBackpressurePolicy(backpressureStrategy),
                backpressureBufferCapacity,
                idempotencyEnabled ? ConnectorIdempotencyPolicy.PRE_FORWARD : ConnectorIdempotencyPolicy.DISABLED,
                ConnectorFailureMode.PROPAGATE),
            idempotencyTracker,
            rejected -> LOG.debugf(
                "Rejected create-to-deliver handoff payloadType=%s",
                rejected == null || rejected.payload() == null ? "null" : rejected.payload().getClass().getName()),
            duplicate -> LOG.debugf(
                "Dropped duplicate create-to-deliver handoff idempotencyKey=%s",
                duplicate == null ? null : duplicate.idempotencyKey()),
            failure -> LOG.error("Create->Deliver connector failed", failure));

        forwardingSubscription = runtime.start();

        LOG.infof("Create->Deliver gRPC ingest bridge started using client %s",
            deliverOrderIngestClient.getClass().getName());
    }

    /**
     * Stops the active forwarding to the DeliverOrder ingest by cancelling the subscription if one exists.
     *
     * Invoked during bean shutdown to release the forwarding resource.
     */
    @PreDestroy
    void onShutdown() {
        if (forwardingSubscription != null) {
            forwardingSubscription.cancel();
        }
    }

    /**
     * Map a pipeline output item to an OrderDispatchSvc.ReadyOrder for the DeliverOrder pipeline.
     *
     * <p>Supports two input shapes: an OrderReadySvc.ReadyOrder (mapped directly) or a
     * com.google.protobuf.Message from which the string fields "order_id", "customer_id", and
     * "ready_at" are read and required to be non-blank.
     *
     * @param item the pipeline output item to map; may be an OrderReadySvc.ReadyOrder, a protobuf Message, or another type
     * @return the mapped OrderDispatchSvc.ReadyOrder if all required fields are present and non-blank; {@code null} otherwise
     */
    private OrderDispatchSvc.ReadyOrder toDeliverReadyOrder(Object item) {
        if (item instanceof OrderReadySvc.ReadyOrder readyOrder) {
            String orderId = readyOrder.getOrderId();
            String customerId = readyOrder.getCustomerId();
            String readyAt = readyOrder.getReadyAt();
            if (orderId.isBlank() || customerId.isBlank() || readyAt.isBlank()) {
                LOG.warnf(
                    "Dropped create->deliver candidate with missing fields signature=%s messageType=%s",
                    ConnectorUtils.failureSignature(
                        "create-to-deliver",
                        "mapping",
                        "missing_required_fields",
                        "na",
                        orderId),
                    readyOrder.getClass().getName());
                return null;
            }
            return OrderDispatchSvc.ReadyOrder.newBuilder()
                .setOrderId(orderId)
                .setCustomerId(customerId)
                .setReadyAt(readyAt)
                .build();
        }
        if (item instanceof Message message) {
            String orderId = ConnectorUtils.readField(message, "order_id");
            String customerId = ConnectorUtils.readField(message, "customer_id");
            String readyAt = ConnectorUtils.readField(message, "ready_at");
            if (!orderId.isBlank() && !customerId.isBlank() && !readyAt.isBlank()) {
                LOG.debugf(
                    "Mapped Message -> ReadyOrder messageType=%s orderId=%s customerId=%s readyAt=%s",
                    message.getClass().getName(), orderId, customerId, readyAt);
                return OrderDispatchSvc.ReadyOrder.newBuilder()
                    .setOrderId(orderId)
                    .setCustomerId(customerId)
                    .setReadyAt(readyAt)
                    .build();
            }
            LOG.warnf(
                "Dropped create->deliver candidate with missing fields signature=%s messageType=%s",
                ConnectorUtils.failureSignature(
                    "create-to-deliver",
                    "mapping",
                    "missing_required_fields",
                    "na",
                    orderId),
                message.getClass().getName());
            return null;
        }
        LOG.warnf(
            "Dropped unsupported create->deliver output item signature=%s type=%s",
            ConnectorUtils.failureSignature(
                "create-to-deliver",
                "mapping",
                "unsupported_item_type",
                "na",
                "na"),
            item == null ? "null" : item.getClass().getName());
        return null;
    }

    /**
     * Map a source record's payload to an OrderDispatchSvc.ReadyOrder and attach dispatch and connector metadata.
     *
     * @param sourceRecord the incoming connector record whose payload will be converted; its dispatch metadata is used as the base for augmentation
     * @return a ConnectorRecord containing the mapped ReadyOrder with ensured dispatch metadata and connector context, or `null` if the payload could not be mapped
     */
    private ConnectorRecord<OrderDispatchSvc.ReadyOrder> mapRecord(ConnectorRecord<Object> sourceRecord) {
        OrderDispatchSvc.ReadyOrder mapped = toDeliverReadyOrder(sourceRecord.payload());
        if (mapped == null) {
            return null;
        }
        return ConnectorRecord.ofPayload(
            mapped,
            ConnectorSupport.ensureDispatchMetadata(
                sourceRecord.dispatchMetadata(),
                "create-to-deliver",
                mapped,
                List.of("orderId", "customerId", "readyAt")),
            Map.of(
                "connector.name", "create-to-deliver",
                "connector.source.step", "Order Ready",
                "connector.target.pipeline", "deliver-order",
                "connector.contract", mapped.getDescriptorForType().getFullName()));
    }
}
