package com.example.smoke.service;

import com.example.smoke.domain.PaymentStatus;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.springframework.stereotype.Component;

@Component
public class PaymentReceiptService {
    public CompletionStage<PaymentStatus> process(PaymentStatus input) {
        if (input == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Payment status must not be null"));
        }
        return CompletableFuture.completedFuture(new PaymentStatus(input.paymentId(), input.status() + ":STAGE"));
    }
}
