package org.pipelineframework.tpfgo.runtime.journey;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

import org.pipelineframework.checkpoint.CheckpointPublicationRequest;

/**
 * Demo-local journey trace endpoint for the TPFGo checkout UI.
 */
@ApplicationScoped
@Path("/checkout/journey")
@Produces(MediaType.APPLICATION_JSON)
public class CheckoutJourneyTraceResource {

    @Inject
    CheckoutJourneyTraceStore traceStore;

    @POST
    @Path("/checkpoints")
    @Consumes(MediaType.APPLICATION_JSON)
    public CheckoutJourneyTraceEvent record(CheckpointPublicationRequest request) {
        try {
            return traceStore.record(request);
        } catch (IllegalArgumentException e) {
            throw new BadRequestException(e.getMessage(), e);
        }
    }

    @GET
    public CheckoutJourneyTraceResponse journey(
        @QueryParam("requestId") String requestId,
        @QueryParam("orderId") String orderId
    ) {
        return new CheckoutJourneyTraceResponse(traceStore.find(requestId, orderId));
    }
}
