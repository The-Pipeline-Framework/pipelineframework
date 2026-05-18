package org.pipelineframework.tpfgo.payment.capture.payment_capture_order.service;

import org.mapstruct.BeanMapping;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;
import org.pipelineframework.tpfgo.common.domain.PaymentCaptured;
import org.pipelineframework.tpfgo.payment.capture.grpc.PipelineTypes;

@org.mapstruct.Mapper(
    componentModel = "jakarta",
    implementationName = "PaymentCapturePaymentCapturedMapperImpl",
    uses = TpfgoGrpcMapStructConverters.class,
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface PaymentCapturedMapper
    extends org.pipelineframework.mapper.Mapper<PaymentCaptured, PipelineTypes.PaymentCaptured> {

    @Mapping(target = "orderId", qualifiedByName = "uuidToString")
    @Mapping(target = "paymentId", qualifiedByName = "uuidToString")
    @Mapping(target = "processedAt", qualifiedByName = "instantToString")
    @Mapping(target = "amount", qualifiedByName = "bigDecimalToString")
    @BeanMapping(ignoreByDefault = true)
    PipelineTypes.PaymentCaptured toGrpc(PaymentCaptured domain);

    @Mapping(target = "orderId", qualifiedByName = "stringToUUID")
    @Mapping(target = "paymentId", qualifiedByName = "stringToUUID")
    @Mapping(target = "processedAt", qualifiedByName = "stringToInstant")
    @Mapping(target = "amount", qualifiedByName = "stringToBigDecimal")
    PaymentCaptured fromGrpc(PipelineTypes.PaymentCaptured grpc);

    @Override
    default PaymentCaptured fromExternal(PipelineTypes.PaymentCaptured external) {
        return fromGrpc(external);
    }

    @Override
    default PipelineTypes.PaymentCaptured toExternal(PaymentCaptured domain) {
        return toGrpc(domain);
    }
}
