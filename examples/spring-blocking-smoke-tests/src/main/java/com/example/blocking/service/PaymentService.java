package com.example.blocking.service;

import com.example.blocking.domain.PaymentRecord;
import com.example.blocking.domain.PaymentStatus;
import org.springframework.stereotype.Component;

@Component
public class PaymentService {
    public PaymentStatus processBlocking(PaymentRecord input) {
        if (input == null) {
            throw new IllegalArgumentException("Payment input must not be null");
        }
        return new PaymentStatus(
            input.paymentId(),
            "APPROVED:" + input.cents());
    }
}
