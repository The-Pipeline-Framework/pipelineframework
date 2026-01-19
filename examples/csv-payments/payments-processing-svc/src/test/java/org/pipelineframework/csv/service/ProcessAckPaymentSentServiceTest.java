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

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pipelineframework.csv.common.domain.AckPaymentSent;
import org.pipelineframework.csv.common.domain.PaymentStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

class ProcessAckPaymentSentServiceTest {

    @Mock private PollAckPaymentSentReactiveService pollAckPaymentSentReactiveService;

    @Mock private AckPaymentSent ackPaymentSent;

    @InjectMocks private ProcessAckPaymentSentService processAckPaymentSentReactiveService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testExecute() {
        // Given
        PaymentStatus expectedStatus = new PaymentStatus();

        when(pollAckPaymentSentReactiveService.process(ackPaymentSent))
                .thenReturn(Uni.createFrom().item(expectedStatus));

        // When
        Uni<PaymentStatus> result = processAckPaymentSentReactiveService.process(ackPaymentSent);

        // Then
        result.subscribe().with(status -> assertEquals(expectedStatus, status));
    }
}
