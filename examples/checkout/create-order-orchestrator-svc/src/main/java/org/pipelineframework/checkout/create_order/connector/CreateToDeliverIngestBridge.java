package org.pipelineframework.checkout.create_order.connector;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

import com.google.protobuf.Message;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import java.util.Objects;
import org.jboss.logging.Logger;
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

    private Cancellable forwardingSubscription;

    /**
     * Creates a bridge that forwards checkpoint outputs from the CreateOrder pipeline to the DeliverOrder ingest client.
     *
     * @param outputBus the source of pipeline checkpoint outputs; must not be null
     * @param deliverOrderIngestClient the gRPC client used to forward ready orders; must not be null
     * @throws NullPointerException if {@code outputBus} or {@code deliverOrderIngestClient} is null
     */
    public CreateToDeliverIngestBridge(PipelineOutputBus outputBus, DeliverOrderIngestClient deliverOrderIngestClient) {
        this.outputBus = Objects.requireNonNull(outputBus, "outputBus must not be null");
        this.deliverOrderIngestClient =
            Objects.requireNonNull(deliverOrderIngestClient, "deliverOrderIngestClient must not be null");
    }

    /**
     * Start forwarding ReadyOrder events from the pipeline output bus to the DeliverOrder ingest client.
     *
     * <p>Subscribes to the application PipelineOutputBus, maps emitted items to
     * OrderDispatchSvc.ReadyOrder when possible, forwards the resulting stream to
     * the DeliverOrderIngestClient, stores the resulting cancellable subscription, and
     * logs the bridge startup.</p>
     *
     * @param ignored the startup event (unused)
     */
    void onStartup(@Observes StartupEvent ignored) {
        Multi<OrderDispatchSvc.ReadyOrder> readyOrderStream = outputBus.stream(Object.class)
            .onItem().transformToMulti(item -> {
                OrderDispatchSvc.ReadyOrder mapped = toDeliverReadyOrder(item);
                return mapped == null ? Multi.createFrom().empty() : Multi.createFrom().item(mapped);
            }).concatenate();

        forwardingSubscription = deliverOrderIngestClient.forward(readyOrderStream);

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
     * Convert a pipeline output item into an OrderDispatchSvc.ReadyOrder suitable for the DeliverOrder pipeline.
     *
     * <p>Supports two input shapes:
     * <ul>
     *   <li>OrderReadySvc.ReadyOrder — mapped directly to the dispatch ReadyOrder.</li>
     *   <li>com.google.protobuf.Message — reads string fields "order_id", "customer_id", and "ready_at" and maps them when all are present and non-blank.</li>
     * </ul>
     *
     * @param item the pipeline output item to map; may be an OrderReadySvc.ReadyOrder, a protobuf Message, or any other type
     * @return the mapped OrderDispatchSvc.ReadyOrder if mapping succeeds; `null` if the item type is unsupported or required fields are missing
     */
    private OrderDispatchSvc.ReadyOrder toDeliverReadyOrder(Object item) {
        if (item instanceof OrderReadySvc.ReadyOrder readyOrder) {
            return OrderDispatchSvc.ReadyOrder.newBuilder()
                .setOrderId(readyOrder.getOrderId())
                .setCustomerId(readyOrder.getCustomerId())
                .setReadyAt(readyOrder.getReadyAt())
                .build();
        }
        if (item instanceof Message message) {
            String orderId = readField(message, "order_id");
            String customerId = readField(message, "customer_id");
            String readyAt = readField(message, "ready_at");
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
                "Dropped create->deliver candidate with missing fields orderId='%s' customerId='%s' readyAt='%s' messageType=%s",
                orderId, customerId, readyAt, message.getClass().getName());
            return null;
        }
        LOG.warnf(
            "Dropped unsupported create->deliver output item type=%s value=%s",
            item == null ? "null" : item.getClass().getName(),
            item);
        return null;
    }

    /**
     * Extracts the named field's value from a protobuf Message and returns it as a String.
     *
     * If the field does not exist, is unset, or is a bytes or nested message type, an empty
     * string is returned. Numeric, boolean, string, and enum field values are converted to
     * their String representation.
     *
     * @param message the protobuf Message to read from
     * @param fieldName the name of the field to read
     * @return the field's string representation if present and of type string, numeric, boolean,
     *         or enum; otherwise an empty string
     */
    private static String readField(Message message, String fieldName) {
        var field = message.getDescriptorForType().findFieldByName(fieldName);
        if (field == null) {
            return "";
        }
        Object value = message.getField(field);
        if (value == null) {
            return "";
        }
        return switch (field.getJavaType()) {
            case STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, ENUM -> String.valueOf(value);
            case BYTE_STRING, MESSAGE -> "";
        };
    }

}