/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.csv.service;

import java.math.BigDecimal;
import java.util.UUID;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import org.junit.jupiter.api.Test;
import org.pipelineframework.csv.common.domain.AckPaymentSent;
import org.pipelineframework.csv.common.domain.PaymentStatus;
import org.pipelineframework.step.NonRetryableException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProcessAckPaymentSentServiceTest {

    @Test
    void processReturnsPolledPaymentStatusWhenProviderCompletes() {
        PaymentStatus expectedStatus =
                new PaymentStatus()
                        .setStatus("Complete")
                        .setReference("ref-101")
                        .setMessage("Mock response")
                        .setPaymentRecordId(UUID.randomUUID())
                        .setAckPaymentSentId(UUID.randomUUID())
                        .setFee(BigDecimal.ONE);

        ProcessAckPaymentSentService service =
                new ProcessAckPaymentSentService(ack -> Uni.createFrom().item(expectedStatus));

        PaymentStatus result = service.process(testAck()).await().indefinitely();

        assertEquals(expectedStatus, result);
    }

    @Test
    void rejectedProviderStatusBecomesNonRetryableFailure() {
        UUID paymentRecordId = UUID.fromString("00000000-0000-0000-0000-000000000101");
        PaymentStatus rejectedStatus =
                new PaymentStatus()
                        .setStatus("Rejected")
                        .setReference("ref-101")
                        .setMessage("Rejected by provider")
                        .setPaymentRecordId(paymentRecordId)
                        .setAckPaymentSentId(UUID.randomUUID())
                        .setFee(BigDecimal.ONE);

        ProcessAckPaymentSentService service =
                new ProcessAckPaymentSentService(ack -> Uni.createFrom().item(rejectedStatus));

        UniAssertSubscriber<PaymentStatus> subscriber =
                service.process(testAck(paymentRecordId)).subscribe().withSubscriber(UniAssertSubscriber.create());

        subscriber.awaitFailure();
        Throwable failure = subscriber.getFailure();
        NonRetryableException nonRetryable = assertInstanceOf(NonRetryableException.class, failure);
        assertTrue(
                nonRetryable
                        .getMessage()
                        .contains("Payment provider returned terminal status 'Rejected'"));
        assertTrue(nonRetryable.getMessage().contains(paymentRecordId.toString()));
    }

    private static AckPaymentSent testAck() {
        return testAck(UUID.randomUUID());
    }

    private static AckPaymentSent testAck(UUID paymentRecordId) {
        return new AckPaymentSent(UUID.randomUUID()).setPaymentRecordId(paymentRecordId);
    }
}
