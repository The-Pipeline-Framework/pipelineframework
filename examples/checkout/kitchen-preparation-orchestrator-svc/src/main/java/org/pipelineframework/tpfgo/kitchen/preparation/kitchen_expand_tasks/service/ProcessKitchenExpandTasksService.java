package org.pipelineframework.tpfgo.kitchen.preparation.kitchen_expand_tasks.service;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Multi;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveStreamingService;
import org.pipelineframework.tpfgo.common.domain.KitchenTask;
import org.pipelineframework.tpfgo.common.domain.OrderAcceptedByRestaurant;
import org.pipelineframework.tpfgo.common.util.DeterministicIds;

@PipelineStep
@ApplicationScoped
public class ProcessKitchenExpandTasksService implements ReactiveStreamingService<OrderAcceptedByRestaurant, KitchenTask> {

    @Override
    public Multi<KitchenTask> process(OrderAcceptedByRestaurant input) {
        if (input == null) {
            return Multi.createFrom().failure(new IllegalArgumentException("input must not be null"));
        }
        List<String> taskNames = List.of("prep", "cook", "pack");
        return Multi.createFrom().range(0, taskNames.size())
            .onItem().transform(index -> new KitchenTask(
                input.orderId(),
                input.customerId(),
                input.restaurantId(),
                input.totalAmount(),
                input.currency(),
                input.kitchenTicketId(),
                DeterministicIds.uuid("kitchen-task", input.orderId().toString(), Integer.toString(index)),
                taskNames.get(index),
                "DONE"));
    }
}
