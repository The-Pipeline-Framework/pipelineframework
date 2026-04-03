package org.pipelineframework.tpfgo.compensation.failure.compensation_finalize_order.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.tpfgo.common.domain.TerminalOrderState;
import org.pipelineframework.tpfgo.common.util.GrpcMappingSupport;
import org.pipelineframework.tpfgo.compensation.failure.grpc.PipelineTypes;

@ApplicationScoped
public class TerminalOrderStateMapper
    implements Mapper<TerminalOrderState, PipelineTypes.TerminalOrderState> {

    @Override
    public TerminalOrderState fromExternal(PipelineTypes.TerminalOrderState external) {
        return new TerminalOrderState(
            GrpcMappingSupport.uuid(external.getOrderId(), "orderId"),
            external.getOutcome(),
            GrpcMappingSupport.instant(external.getResolvedAt(), "resolvedAt"),
            external.getResolutionAction(),
            external.getPaymentStatus(),
            GrpcMappingSupport.uuid(external.getPaymentId(), "paymentId"),
            external.getFailureCode());
    }

    @Override
    public PipelineTypes.TerminalOrderState toExternal(TerminalOrderState domain) {
        var builder = PipelineTypes.TerminalOrderState.newBuilder()
            .setOrderId(GrpcMappingSupport.str(domain.orderId()))
            .setOutcome(domain.outcome())
            .setResolvedAt(GrpcMappingSupport.str(domain.resolvedAt()))
            .setResolutionAction(domain.resolutionAction())
            .setPaymentStatus(domain.paymentStatus());
        if (domain.paymentId() != null) {
            builder.setPaymentId(GrpcMappingSupport.str(domain.paymentId()));
        }
        if (domain.failureCode() != null) {
            builder.setFailureCode(domain.failureCode());
        }
        return builder.build();
    }
}
