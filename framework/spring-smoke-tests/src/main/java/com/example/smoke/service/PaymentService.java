package com.example.smoke.service;

import com.example.smoke.domain.PaymentRecord;
import com.example.smoke.domain.PaymentStatus;
import io.smallrye.mutiny.Uni;
import org.springframework.stereotype.Component;

@Component
public class PaymentService {
    public Uni<PaymentStatus> process(PaymentRecord input) {
        if (input == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("Payment input must not be null"));
        }
        return Uni.createFrom().item(new PaymentStatus(input.paymentId(), "APPROVED:" + input.cents()));
    }
}
