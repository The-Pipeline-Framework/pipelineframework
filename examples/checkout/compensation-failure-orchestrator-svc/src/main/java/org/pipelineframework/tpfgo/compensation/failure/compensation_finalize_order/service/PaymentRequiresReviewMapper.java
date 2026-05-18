package org.pipelineframework.tpfgo.compensation.failure.compensation_finalize_order.service;

import org.mapstruct.BeanMapping;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;
import org.pipelineframework.tpfgo.common.domain.PaymentRequiresReview;
import org.pipelineframework.tpfgo.compensation.failure.grpc.PipelineTypes;

@org.mapstruct.Mapper(
    componentModel = "jakarta",
    implementationName = "CompensationFailurePaymentRequiresReviewMapperImpl",
    uses = TpfgoGrpcMapStructConverters.class,
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface PaymentRequiresReviewMapper
    extends org.pipelineframework.mapper.Mapper<PaymentRequiresReview, PipelineTypes.PaymentRequiresReview> {

    @Mapping(target = "orderId", qualifiedByName = "uuidToString")
    @Mapping(target = "processedAt", qualifiedByName = "instantToString")
    @Mapping(target = "amount", qualifiedByName = "bigDecimalToString")
    @BeanMapping(ignoreByDefault = true)
    PipelineTypes.PaymentRequiresReview toGrpc(PaymentRequiresReview domain);

    @Mapping(target = "orderId", qualifiedByName = "stringToUUID")
    @Mapping(target = "processedAt", qualifiedByName = "stringToInstant")
    @Mapping(target = "amount", qualifiedByName = "stringToBigDecimal")
    PaymentRequiresReview fromGrpc(PipelineTypes.PaymentRequiresReview grpc);

    @Override
    default PaymentRequiresReview fromExternal(PipelineTypes.PaymentRequiresReview external) {
        return fromGrpc(external);
    }

    @Override
    default PipelineTypes.PaymentRequiresReview toExternal(PaymentRequiresReview domain) {
        return toGrpc(domain);
    }
}
