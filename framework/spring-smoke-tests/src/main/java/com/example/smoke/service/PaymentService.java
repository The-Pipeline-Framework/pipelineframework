package com.example.smoke.service;

import com.example.smoke.domain.PaymentRecord;
import com.example.smoke.domain.PaymentStatus;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
public class PaymentService {
    public Mono<PaymentStatus> process(PaymentRecord input) {
        if (input == null) {
            return Mono.error(new IllegalArgumentException("Payment input must not be null"));
        }
        return Mono.just(new PaymentStatus(input.paymentId(), "APPROVED:" + input.cents()));
    }
}
