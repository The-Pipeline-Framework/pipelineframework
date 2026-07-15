package com.example.smoke.mapper;

import com.example.smoke.domain.PaymentRecord;
import com.example.smoke.dto.PaymentRecordDto;
import org.pipelineframework.mapper.Mapper;
import org.springframework.stereotype.Component;

@Component
public class PaymentRecordMapper implements Mapper<PaymentRecord, PaymentRecordDto> {
    @Override
    public PaymentRecord fromExternal(PaymentRecordDto external) {
        return new PaymentRecord(external.paymentId(), external.cents());
    }

    @Override
    public PaymentRecordDto toExternal(PaymentRecord domain) {
        return new PaymentRecordDto(domain.paymentId(), domain.cents());
    }
}
