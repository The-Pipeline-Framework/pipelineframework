package com.example.smoke.service;

import com.example.smoke.domain.PaymentStatus;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
public class PaymentAuditService {
    public Mono<PaymentStatus> audit(PaymentStatus input) {
        if (input == null) {
            return Mono.error(new IllegalArgumentException("Payment status must not be null"));
        }
        return Mono.just(new PaymentStatus(input.paymentId(), input.status() + ":DELEGATED"));
    }
}
