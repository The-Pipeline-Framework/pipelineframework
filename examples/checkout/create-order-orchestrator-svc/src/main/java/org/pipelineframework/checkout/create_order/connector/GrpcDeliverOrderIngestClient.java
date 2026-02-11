package org.pipelineframework.checkout.create_order.connector;

import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.grpc.GrpcClient;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import java.time.Duration;
import org.jboss.logging.Logger;
import org.pipelineframework.checkout.deliverorder.grpc.MutinyOrchestratorServiceGrpc;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDispatchSvc;

@ApplicationScoped
public class GrpcDeliverOrderIngestClient implements DeliverOrderIngestClient {

    private static final Logger LOG = Logger.getLogger(GrpcDeliverOrderIngestClient.class);

    @GrpcClient("deliver-order-orchestrator")
    MutinyOrchestratorServiceGrpc.MutinyOrchestratorServiceStub deliverOrchestratorClient;

    @Override
    public Cancellable forward(Multi<OrderDispatchSvc.ReadyOrder> readyOrderStream) {
        LOG.info("Starting gRPC forwarding stream to deliver-order orchestrator ingest");
        return deliverOrchestratorClient.ingest(readyOrderStream)
            .onFailure().retry().withBackOff(Duration.ofMillis(100), Duration.ofSeconds(1)).atMost(20)
            .subscribe().with(
                delivered -> LOG.debugf("Forwarded order %s to deliver pipeline", delivered.getOrderId()),
                error -> LOG.error("Create->Deliver forwarding subscription terminated", error));
    }
}
