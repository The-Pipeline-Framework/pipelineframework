package org.pipelineframework.tpfgo.checkout.checkout_validate_request.service;

import java.time.Clock;
import java.time.Instant;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.tpfgo.common.domain.PlaceOrderRequest;
import org.pipelineframework.tpfgo.common.domain.ValidatedOrderRequest;

@PipelineStep(
    inputType = PlaceOrderRequest.class,
    outputType = ValidatedOrderRequest.class,
    stepType = org.pipelineframework.step.StepOneToOne.class,
    backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class,
    inboundMapper = org.pipelineframework.tpfgo.checkout.checkout_validate_request.service.PlaceOrderRequestMapper.class,
    outboundMapper = org.pipelineframework.tpfgo.checkout.checkout_validate_request.service.ValidatedOrderRequestMapper.class
)
@ApplicationScoped
public class ProcessCheckoutValidateRequestService implements ReactiveService<PlaceOrderRequest, ValidatedOrderRequest> {

    private final Clock clock;

    @Inject
    public ProcessCheckoutValidateRequestService(Clock clock) {
        this.clock = clock;
    }

    @Override
    public Uni<ValidatedOrderRequest> process(PlaceOrderRequest input) {
        if (input == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("input must not be null"));
        }
        if (input.totalAmount().signum() < 0) {
            return Uni.createFrom().failure(new IllegalArgumentException("totalAmount must be >= 0"));
        }
        return Uni.createFrom().item(new ValidatedOrderRequest(
            input.requestId(),
            input.customerId(),
            input.restaurantId(),
            input.items(),
            input.totalAmount(),
            input.currency(),
            Instant.now(clock)));
    }
}
