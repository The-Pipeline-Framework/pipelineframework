package org.pipelineframework.checkout.create_order.connector;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

import com.google.protobuf.Message;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
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

    public CreateToDeliverIngestBridge(PipelineOutputBus outputBus, DeliverOrderIngestClient deliverOrderIngestClient) {
        this.outputBus = outputBus;
        this.deliverOrderIngestClient = deliverOrderIngestClient;
    }

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

    @PreDestroy
    void onShutdown() {
        if (forwardingSubscription != null) {
            forwardingSubscription.cancel();
        }
    }

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
