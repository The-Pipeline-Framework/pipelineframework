package org.example.pipeline;

import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import java.util.List;
import org.pipelineframework.PipelineRunner;
import org.pipelineframework.repository.PayloadReference;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;

/** Test admission path: the real PipelineRunner carries one canonical reference through a typed step. */
@Path("/test/owned-payload/admit")
public final class OwnedReferenceAdmissionResource {
    @Inject
    PipelineRunner runner;

    @POST
    @Consumes("application/json")
    @Produces("application/json")
    @SuppressWarnings("unchecked")
    public Uni<ReceiptOutput> admit(InvoiceInput input) {
        return (Uni<ReceiptOutput>) runner.run(Uni.createFrom().item(input), List.of(new CarryReferenceStep()));
    }

    public record InvoiceInput(PayloadReference payload_ref) {
    }

    public record ReceiptOutput(PayloadReference payload_ref) {
    }

    /** Business transformation has no multipart, storage, access policy, or HTTP concerns. */
    public static final class CarryReferenceStep extends ConfigurableStep
        implements StepOneToOne<InvoiceInput, ReceiptOutput> {
        @Override
        public Uni<ReceiptOutput> applyOneToOne(InvoiceInput input) {
            return Uni.createFrom().item(new ReceiptOutput(input.payload_ref()));
        }
    }
}
