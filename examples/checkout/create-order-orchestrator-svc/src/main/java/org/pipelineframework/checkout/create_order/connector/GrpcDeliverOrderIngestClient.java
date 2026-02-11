package org.pipelineframework.checkout.create_order.connector;

import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.grpc.GrpcClient;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import org.jboss.logging.Logger;
import org.pipelineframework.checkout.deliverorder.grpc.MutinyOrchestratorServiceGrpc;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDispatchSvc;

@ApplicationScoped
public class GrpcDeliverOrderIngestClient implements DeliverOrderIngestClient {

    private static final Logger LOG = Logger.getLogger(GrpcDeliverOrderIngestClient.class);

    @GrpcClient("deliver-order-orchestrator")
    MutinyOrchestratorServiceGrpc.MutinyOrchestratorServiceStub deliverOrchestratorClient;

    /**
     * Starts forwarding a stream of ready orders to the deliver-order orchestrator ingest.
     *
     * @param readyOrderStream a reactive stream of ReadyOrder messages to send to the orchestrator
     * @return a {@link Cancellable} representing the active subscription; cancelling it stops the forwarding
     */
    @Override
    public Cancellable forward(Multi<OrderDispatchSvc.ReadyOrder> readyOrderStream) {
        LOG.info("Starting gRPC forwarding stream to deliver-order orchestrator ingest");
        return deliverOrchestratorClient.ingest(readyOrderStream)
            .subscribe().with(
                delivered -> LOG.debugf("Forwarded order %s to deliver pipeline", delivered.getOrderId()),
                error -> LOG.error("Create->Deliver forwarding subscription terminated", error));
    }
}