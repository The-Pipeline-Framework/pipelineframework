package org.pipelineframework.tpfgo.payment.capture.payment_capture_order.service;

import org.mapstruct.BeanMapping;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;
import org.pipelineframework.tpfgo.common.domain.PaymentRejected;
import org.pipelineframework.tpfgo.payment.capture.grpc.PipelineTypes;

@org.mapstruct.Mapper(
    componentModel = "jakarta",
    uses = TpfgoGrpcMapStructConverters.class,
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface PaymentRejectedMapper
    extends org.pipelineframework.mapper.Mapper<PaymentRejected, PipelineTypes.PaymentRejected> {

    @Mapping(target = "orderId", qualifiedByName = "uuidToString")
    @Mapping(target = "processedAt", qualifiedByName = "instantToString")
    @Mapping(target = "amount", qualifiedByName = "bigDecimalToString")
    @BeanMapping(ignoreByDefault = true)
    PipelineTypes.PaymentRejected toGrpc(PaymentRejected domain);

    @Mapping(target = "orderId", qualifiedByName = "stringToUUID")
    @Mapping(target = "processedAt", qualifiedByName = "stringToInstant")
    @Mapping(target = "amount", qualifiedByName = "stringToBigDecimal")
    PaymentRejected fromGrpc(PipelineTypes.PaymentRejected grpc);

    @Override
    default PaymentRejected fromExternal(PipelineTypes.PaymentRejected external) {
        return fromGrpc(external);
    }

    @Override
    default PipelineTypes.PaymentRejected toExternal(PaymentRejected domain) {
        return toGrpc(domain);
    }
}
