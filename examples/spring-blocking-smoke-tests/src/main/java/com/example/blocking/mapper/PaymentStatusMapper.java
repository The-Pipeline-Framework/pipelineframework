package com.example.blocking.mapper;

import com.example.blocking.domain.PaymentStatus;
import com.example.blocking.dto.PaymentStatusDto;
import org.pipelineframework.mapper.Mapper;
import org.springframework.stereotype.Component;

@Component
public class PaymentStatusMapper implements Mapper<PaymentStatus, PaymentStatusDto> {
    @Override
    public PaymentStatus fromExternal(PaymentStatusDto external) {
        return new PaymentStatus(external.paymentId(), external.status(), external.virtualThread());
    }

    @Override
    public PaymentStatusDto toExternal(PaymentStatus domain) {
        return new PaymentStatusDto(domain.paymentId(), domain.status(), domain.virtualThread());
    }
}
