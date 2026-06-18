package com.example.smoke.mapper;

import com.example.smoke.domain.PaymentStatus;
import com.example.smoke.dto.PaymentStatusDto;
import org.pipelineframework.mapper.Mapper;
import org.springframework.stereotype.Component;

@Component
public class PaymentStatusMapper implements Mapper<PaymentStatus, PaymentStatusDto> {
    @Override
    public PaymentStatus fromExternal(PaymentStatusDto external) {
        return new PaymentStatus(external.paymentId(), external.status());
    }

    @Override
    public PaymentStatusDto toExternal(PaymentStatus domain) {
        return new PaymentStatusDto(domain.paymentId(), domain.status());
    }
}
