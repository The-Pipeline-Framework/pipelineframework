package org.pipelineframework.tpfgo.kitchen.preparation.kitchen_reduce_completion.service;

import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveStreamingClientService;
import org.pipelineframework.tpfgo.common.domain.KitchenTask;
import org.pipelineframework.tpfgo.common.domain.OrderReadyForDispatch;

@PipelineStep(
    inputType = KitchenTask.class,
    outputType = OrderReadyForDispatch.class,
    stepType = org.pipelineframework.step.StepManyToOne.class,
    backendType = org.pipelineframework.grpc.GrpcServiceClientStreamingAdapter.class,
    inboundMapper = org.pipelineframework.tpfgo.kitchen.preparation.kitchen_expand_tasks.service.KitchenTaskMapper.class,
    outboundMapper = org.pipelineframework.tpfgo.kitchen.preparation.kitchen_reduce_completion.service.OrderReadyForDispatchMapper.class
)
@ApplicationScoped
public class ProcessKitchenReduceCompletionService
    implements ReactiveStreamingClientService<KitchenTask, OrderReadyForDispatch> {

    private final Clock clock;

    @Inject
    public ProcessKitchenReduceCompletionService(Clock clock) {
        this.clock = clock;
    }

    @Override
    public Uni<OrderReadyForDispatch> process(Multi<KitchenTask> input) {
        return input.collect().asList().onItem().transform(tasks -> {
            if (tasks == null || tasks.isEmpty()) {
                throw new IllegalArgumentException("tasks must not be empty");
            }
            var ordered = tasks.stream()
                .sorted(Comparator.comparing(task -> task.taskId().toString()))
                .toList();
            KitchenTask first = ordered.getFirst();
            String lineageDigest = ordered.stream()
                .map(task -> task.taskId().toString())
                .collect(Collectors.joining("|"));
            return new OrderReadyForDispatch(
                first.orderId(),
                first.customerId(),
                first.restaurantId(),
                first.totalAmount(),
                first.currency(),
                Instant.now(clock),
                first.kitchenTicketId(),
                lineageDigest);
        });
    }
}
