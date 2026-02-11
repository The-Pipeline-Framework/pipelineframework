package org.pipelineframework.checkout.deliver_order.connector;

import com.google.protobuf.Message;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.pipelineframework.PipelineOutputBus;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDeliveredSvc;

@ApplicationScoped
public class DeliverToNextIngestBridge {

    private static final Logger LOG = Logger.getLogger(DeliverToNextIngestBridge.class);

    private final PipelineOutputBus outputBus;
    private final DeliveredOrderForwardClient forwardClient;
    private final boolean enabled;

    private Cancellable forwardingSubscription;

    public DeliverToNextIngestBridge(
        PipelineOutputBus outputBus,
        DeliveredOrderForwardClient forwardClient,
        @ConfigProperty(name = "checkout.deliver.forward.enabled", defaultValue = "false") boolean enabled
    ) {
        this.outputBus = outputBus;
        this.forwardClient = forwardClient;
        this.enabled = enabled;
    }

    void onStartup(@Observes StartupEvent ignored) {
        if (!enabled) {
            LOG.info("Deliver->next forwarding bridge disabled (checkout.deliver.forward.enabled=false)");
            return;
        }

        Multi<OrderDeliveredSvc.DeliveredOrder> deliveredStream = outputBus.stream(Object.class)
            .onItem().transformToMulti(item -> {
                OrderDeliveredSvc.DeliveredOrder mapped = toDeliveredOrder(item);
                return mapped == null ? Multi.createFrom().empty() : Multi.createFrom().item(mapped);
            }).concatenate();

        forwardingSubscription = forwardClient.forward(
            deliveredStream.onFailure().invoke(error ->
                LOG.error("Deliver->next stream failed before forwarding", error))
        );
        LOG.infof("Deliver->next forwarding bridge started using client %s", forwardClient.getClass().getName());
    }

    @PreDestroy
    void onShutdown() {
        if (forwardingSubscription != null) {
            forwardingSubscription.cancel();
        }
    }

    private OrderDeliveredSvc.DeliveredOrder toDeliveredOrder(Object item) {
        if (item instanceof OrderDeliveredSvc.DeliveredOrder delivered) {
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
            "Dropped unsupported deliver->next item type=%s value=%s",
            item == null ? "null" : item.getClass().getName(),
            item);
        return null;
    }

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
