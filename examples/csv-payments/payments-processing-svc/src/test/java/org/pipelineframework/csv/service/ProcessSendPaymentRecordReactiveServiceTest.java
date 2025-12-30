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

import java.util.concurrent.Callable;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pipelineframework.csv.common.domain.AckPaymentSent;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.mapper.SendPaymentRequestMapper;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class ProcessSendPaymentRecordReactiveServiceTest {

    @Mock private PaymentProviderServiceMock paymentProviderServiceMock;
    @Mock private PaymentRecord paymentRecord;
    @Mock private Vertx vertx; // 👈 mock Vertx itself

    @InjectMocks private ProcessSendPaymentRecordReactiveService processSendPaymentRecordReactiveService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        // Make executeBlocking immediately run the callable and return a succeeded Uni
        when(vertx.executeBlocking(any(Callable.class)))
                .thenAnswer(
                        invocation -> {
                            Callable<?> callable = invocation.getArgument(0);
                            try {
                                Object result = callable.call();
                                // Mutiny Vertx returns io.smallrye.mutiny.Uni<T>
                                return Uni.createFrom().item(result);
                            } catch (Exception e) {
                                return Uni.createFrom().failure(e);
                            }
                        });
    }

    @Test
    void testExecute() {
        // Given
        AckPaymentSent expectedAck = new AckPaymentSent();

        when(paymentProviderServiceMock.sendPayment(
                        any(SendPaymentRequestMapper.SendPaymentRequest.class)))
                .thenReturn(expectedAck);

        // When
        Uni<AckPaymentSent> result = processSendPaymentRecordReactiveService.process(paymentRecord);

        // Then
        result.subscribe().with(ack -> Assertions.assertEquals(expectedAck, ack));
    }
}
