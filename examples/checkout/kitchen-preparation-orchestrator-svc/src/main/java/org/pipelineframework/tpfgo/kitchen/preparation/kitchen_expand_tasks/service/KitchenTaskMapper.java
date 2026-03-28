package org.pipelineframework.tpfgo.kitchen.preparation.kitchen_expand_tasks.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.tpfgo.common.domain.KitchenTask;
import org.pipelineframework.tpfgo.common.util.GrpcMappingSupport;
import org.pipelineframework.tpfgo.kitchen.preparation.grpc.KitchenExpandTasksSvc;

@ApplicationScoped
public class KitchenTaskMapper implements Mapper<KitchenTask, KitchenExpandTasksSvc.KitchenTask> {

    @Override
    public KitchenTask fromExternal(KitchenExpandTasksSvc.KitchenTask external) {
        return new KitchenTask(
            GrpcMappingSupport.uuid(external.getOrderId(), "orderId"),
            GrpcMappingSupport.uuid(external.getCustomerId(), "customerId"),
            GrpcMappingSupport.uuid(external.getRestaurantId(), "restaurantId"),
            GrpcMappingSupport.decimal(external.getTotalAmount(), "totalAmount"),
            external.getCurrency(),
            GrpcMappingSupport.uuid(external.getKitchenTicketId(), "kitchenTicketId"),
            GrpcMappingSupport.uuid(external.getTaskId(), "taskId"),
            external.getTaskName(),
            external.getTaskStatus());
    }

    @Override
    public KitchenExpandTasksSvc.KitchenTask toExternal(KitchenTask domain) {
        return KitchenExpandTasksSvc.KitchenTask.newBuilder()
            .setOrderId(GrpcMappingSupport.str(domain.orderId()))
            .setCustomerId(GrpcMappingSupport.str(domain.customerId()))
            .setRestaurantId(GrpcMappingSupport.str(domain.restaurantId()))
            .setTotalAmount(GrpcMappingSupport.str(domain.totalAmount()))
            .setCurrency(domain.currency())
            .setKitchenTicketId(GrpcMappingSupport.str(domain.kitchenTicketId()))
            .setTaskId(GrpcMappingSupport.str(domain.taskId()))
            .setTaskName(domain.taskName())
            .setTaskStatus(domain.taskStatus())
            .build();
    }
}
