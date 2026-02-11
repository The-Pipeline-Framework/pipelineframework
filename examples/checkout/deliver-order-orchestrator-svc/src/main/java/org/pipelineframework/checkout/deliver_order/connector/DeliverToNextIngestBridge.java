package org.pipelineframework.checkout.deliver_order.connector;

import com.google.protobuf.Message;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.time.Duration;
import java.util.Objects;
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
            .onItem().transform(this::toDeliveredOrder)
            .select().where(Objects::nonNull)
            .onFailure().invoke(error -> LOG.error("Deliver->next stream failed before forwarding", error))
            .onFailure().retry().withBackOff(Duration.ofMillis(100), Duration.ofSeconds(1)).atMost(5);

        forwardingSubscription = forwardClient.forward(deliveredStream);
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
            "Dropped unsupported deliver->next item type=%s",
            item == null ? "null" : item.getClass().getName());
        return null;
    }

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
