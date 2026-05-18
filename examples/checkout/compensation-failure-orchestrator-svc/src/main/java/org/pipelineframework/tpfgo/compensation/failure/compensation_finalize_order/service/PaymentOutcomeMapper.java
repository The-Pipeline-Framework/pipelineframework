package org.pipelineframework.tpfgo.compensation.failure.compensation_finalize_order.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.tpfgo.common.domain.PaymentCaptured;
import org.pipelineframework.tpfgo.common.domain.PaymentOutcome;
import org.pipelineframework.tpfgo.common.domain.PaymentOutcomeVisitor;
import org.pipelineframework.tpfgo.common.domain.PaymentRejected;
import org.pipelineframework.tpfgo.common.domain.PaymentRequiresReview;
import org.pipelineframework.tpfgo.common.util.GrpcMappingSupport;
import org.pipelineframework.tpfgo.compensation.failure.grpc.PipelineTypes;

@ApplicationScoped
public class PaymentOutcomeMapper
    implements Mapper<PaymentOutcome, PipelineTypes.PaymentOutcome> {

    @Override
    public PaymentOutcome fromExternal(PipelineTypes.PaymentOutcome external) {
        return switch (external.getOutcomeCase()) {
            case CAPTURED -> new PaymentCaptured(
                GrpcMappingSupport.uuid(external.getCaptured().getOrderId(), "orderId"),
                GrpcMappingSupport.uuid(external.getCaptured().getPaymentId(), "paymentId"),
                GrpcMappingSupport.instant(external.getCaptured().getProcessedAt(), "processedAt"),
                GrpcMappingSupport.decimal(external.getCaptured().getAmount(), "amount"),
                external.getCaptured().getCurrency());
            case REJECTED -> new PaymentRejected(
                GrpcMappingSupport.uuid(external.getRejected().getOrderId(), "orderId"),
                GrpcMappingSupport.instant(external.getRejected().getProcessedAt(), "processedAt"),
                GrpcMappingSupport.decimal(external.getRejected().getAmount(), "amount"),
                external.getRejected().getCurrency(),
                external.getRejected().getFailureCode(),
                external.getRejected().getFailureReason());
            case REQUIRES_REVIEW -> new PaymentRequiresReview(
                GrpcMappingSupport.uuid(external.getRequiresReview().getOrderId(), "orderId"),
                GrpcMappingSupport.instant(external.getRequiresReview().getProcessedAt(), "processedAt"),
                GrpcMappingSupport.decimal(external.getRequiresReview().getAmount(), "amount"),
                external.getRequiresReview().getCurrency(),
                external.getRequiresReview().getReviewReason());
            case OUTCOME_NOT_SET -> throw new IllegalArgumentException("PaymentOutcome has no selected variant");
        };
    }

    @Override
    public PipelineTypes.PaymentOutcome toExternal(PaymentOutcome domain) {
        var builder = PipelineTypes.PaymentOutcome.newBuilder();
        return domain.accept(new PaymentOutcomeVisitor<>() {
            @Override
            public PipelineTypes.PaymentOutcome captured(PaymentCaptured captured) {
                return builder.setCaptured(PipelineTypes.PaymentCaptured.newBuilder()
                    .setOrderId(GrpcMappingSupport.str(captured.orderId()))
                    .setPaymentId(GrpcMappingSupport.str(captured.paymentId()))
                    .setProcessedAt(GrpcMappingSupport.str(captured.processedAt()))
                    .setAmount(GrpcMappingSupport.str(captured.amount()))
                    .setCurrency(captured.currency())
                    .build()).build();
            }

            @Override
            public PipelineTypes.PaymentOutcome rejected(PaymentRejected rejected) {
                return builder.setRejected(PipelineTypes.PaymentRejected.newBuilder()
                    .setOrderId(GrpcMappingSupport.str(rejected.orderId()))
                    .setProcessedAt(GrpcMappingSupport.str(rejected.processedAt()))
                    .setAmount(GrpcMappingSupport.str(rejected.amount()))
                    .setCurrency(rejected.currency())
                    .setFailureCode(rejected.failureCode())
                    .setFailureReason(rejected.failureReason())
                    .build()).build();
            }

            @Override
            public PipelineTypes.PaymentOutcome requiresReview(PaymentRequiresReview review) {
                return builder.setRequiresReview(PipelineTypes.PaymentRequiresReview.newBuilder()
                    .setOrderId(GrpcMappingSupport.str(review.orderId()))
                    .setProcessedAt(GrpcMappingSupport.str(review.processedAt()))
                    .setAmount(GrpcMappingSupport.str(review.amount()))
                    .setCurrency(review.currency())
                    .setReviewReason(review.reviewReason())
                    .build()).build();
            }
        });
    }
}
