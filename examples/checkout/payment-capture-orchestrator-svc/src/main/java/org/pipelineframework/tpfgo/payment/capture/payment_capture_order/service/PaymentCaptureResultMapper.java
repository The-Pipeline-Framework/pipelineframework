package org.pipelineframework.tpfgo.payment.capture.payment_capture_order.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.tpfgo.common.domain.PaymentCaptureResult;
import org.pipelineframework.tpfgo.common.util.GrpcMappingSupport;
import org.pipelineframework.tpfgo.payment.capture.grpc.PaymentCaptureOrderSvc;

@ApplicationScoped
public class PaymentCaptureResultMapper
    implements Mapper<PaymentCaptureResult, PaymentCaptureOrderSvc.PaymentCaptureResult> {

    @Override
    public PaymentCaptureResult fromExternal(PaymentCaptureOrderSvc.PaymentCaptureResult external) {
        return new PaymentCaptureResult(
            GrpcMappingSupport.uuid(external.getOrderId(), "orderId"),
            GrpcMappingSupport.uuid(external.getPaymentId(), "paymentId"),
            GrpcMappingSupport.instant(external.getProcessedAt(), "processedAt"),
            GrpcMappingSupport.decimal(external.getAmount(), "amount"),
            external.getCurrency(),
            external.getStatus(),
            external.getFailureCode(),
            external.getFailureReason());
    }

    @Override
    public PaymentCaptureOrderSvc.PaymentCaptureResult toExternal(PaymentCaptureResult domain) {
        var builder = PaymentCaptureOrderSvc.PaymentCaptureResult.newBuilder()
            .setOrderId(GrpcMappingSupport.str(domain.orderId()))
            .setProcessedAt(GrpcMappingSupport.str(domain.processedAt()))
            .setAmount(GrpcMappingSupport.str(domain.amount()))
            .setCurrency(domain.currency())
            .setStatus(domain.status());
        if (domain.paymentId() != null) {
            builder.setPaymentId(GrpcMappingSupport.str(domain.paymentId()));
        }
        if (domain.failureCode() != null) {
            builder.setFailureCode(domain.failureCode());
        }
        if (domain.failureReason() != null) {
            builder.setFailureReason(domain.failureReason());
        }
        return builder.build();
    }
}
